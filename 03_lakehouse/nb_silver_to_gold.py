"""
Retail360 — nb_silver_to_gold (Fabric Lakehouse notebook)
==========================================================
Run this notebook in retail360_lh after:
  1. PostgreSQL Mirroring is active (silver_pg_* visible)
  2. Dataflow Gen2 has run (silver_weather, silver_promotions,
     silver_competitor_prices populated)
  3. Databricks notebooks completed (silver_adx_* visible)

Cells in order:
  1  Verify all source tables exist
  2  Fix silver_weather column names (match ontology)
  3  Fix competitor price variance (realistic positioning)
  4  Fix promo names to English
  5  Build gold_sales_daily
  6  Build gold_inventory_status (English status, coverage_days)
  7  Build gold_forecast_vs_actual
  8  Cast all DECIMAL → DOUBLE (Fabric IQ GQL requirement)
  9  Build pre-computed Gold tables
  10 Build dimension tables
  11 Final validation
"""

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, DateType
import pandas as pd
import numpy as np
import math
import random
from datetime import datetime, timedelta

random.seed(42)
np.random.seed(42)

# ── Cell 1 — Verify sources ───────────────────────────────────

required = [
    "silver_pg_stores","silver_pg_products",
    "silver_pg_orders","silver_pg_order_items",
    "silver_weather","silver_promotions","silver_competitor_prices",
]
for t in required:
    try:
        cnt = spark.table(t).count()
        print(f"  ✓ {t}: {cnt:,} rows")
    except Exception as e:
        print(f"  ✗ {t}: MISSING — {e}")

# ── Cell 2 — Fix silver_weather column names ──────────────────
# Rename to match Fabric IQ ontology property names

weather = spark.table("silver_weather")
print("Columns before:", weather.columns)

rename_map = {
    "precipitation_mm": "rainfall_mm",
    "temp_max_c":        "max_temperature_c",
    "temp_min_c":        "min_temperature_c",
    "wind_kmh":          "wind_speed_kmh",
    "weather_label":     "weather_condition",
}
for old, new in rename_map.items():
    if old in weather.columns and new not in weather.columns:
        weather = weather.withColumnRenamed(old, new)
        print(f"  Renamed: {old} → {new}")
    elif new in weather.columns:
        print(f"  Already renamed: {new} ✓")

weather.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_weather")
print("✓ Cell 2 done — silver_weather columns aligned")

# ── Cell 3 — Fix competitor prices ───────────────────────────
# Regenerate with realistic competitor positioning

comp_pd = spark.table("silver_competitor_prices").toPandas()

profiles = {
    "MaxiMarché":   {"bias": -0.05, "std": 0.08},
    "PrimoShop":    {"bias":  0.03, "std": 0.06},
    "BonPrix":      {"bias": -0.10, "std": 0.10},
    "NeoShop":      {"bias":  0.02, "std": 0.09},
    "TendancePlus": {"bias": -0.02, "std": 0.07},
}

def regen_gap(row):
    p = profiles.get(row["competitor"], {"bias":0.0,"std":0.08})
    return round(float(np.random.normal(p["bias"], p["std"])) * 100, 2)

comp_pd["price_gap_pct"]    = comp_pd.apply(regen_gap, axis=1)
comp_pd["competitor_price"] = (
    comp_pd["our_price"].astype(float)
    * (1 + comp_pd["price_gap_pct"] / 100)
).round(2).clip(lower=0.5)
comp_pd["is_cheaper"] = comp_pd["price_gap_pct"] < 0

spark.createDataFrame(comp_pd).write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_competitor_prices")

spark.sql("""
    SELECT competitor,
           ROUND(AVG(price_gap_pct),2) AS avg_gap,
           SUM(CASE WHEN is_cheaper THEN 1 ELSE 0 END) AS cheaper_count
    FROM silver_competitor_prices
    GROUP BY competitor ORDER BY avg_gap
""").show()
print("✓ Cell 3 done — competitor prices fixed")

# ── Cell 4 — Fix promo names to English ──────────────────────

promos = spark.table("silver_promotions")
promos_fixed = promos.withColumn("promo_name",
    F.when(F.col("promo_name") == "Soldes Hiver",         "Winter Sales")
     .when(F.col("promo_name") == "Soldes Été",           "Summer Sales")
     .when(F.col("promo_name") == "Rentrée scolaire",     "Back to School")
     .when(F.col("promo_name") == "Saint-Valentin",       "Valentine's Day")
     .when(F.col("promo_name") == "Fêtes de fin d'année", "Christmas")
     .otherwise(F.col("promo_name"))
)
promos_fixed.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true").saveAsTable("silver_promotions")

spark.sql("SELECT DISTINCT promo_name FROM silver_promotions ORDER BY 1").show()
print("✓ Cell 4 done — promo names in English")

# ── Cell 5 — Build gold_sales_daily ──────────────────────────
# Aggregated at store/category/date/channel — NO product_id column

gold_sales = spark.sql("""
    SELECT
        o.store_id,
        s.name                                              AS store_name,
        s.region,
        s.surface_m2,
        s.store_type,
        o.order_date                                        AS sales_date,
        o.channel,
        p.category,
        p.subcategory,
        COUNT(DISTINCT o.order_id)                          AS nb_orders,
        SUM(oi.quantity)                                    AS units_sold,
        ROUND(SUM(oi.quantity * oi.unit_price
              * (1 - oi.discount_pct / 100)), 2)           AS revenue,
        ROUND(SUM(oi.quantity
              * (oi.unit_price - p.unit_cost)
              * (1 - oi.discount_pct / 100)), 2)           AS gross_margin,
        ROUND(AVG(oi.discount_pct), 2)                     AS avg_discount_pct,
        CAST(CASE WHEN pr.promo_name IS NOT NULL
             THEN 1 ELSE 0 END AS INT)                     AS is_promo_day,
        pr.promo_name
    FROM silver_pg_orders o
    JOIN silver_pg_order_items oi ON o.order_id   = oi.order_id
    JOIN silver_pg_stores s       ON o.store_id   = s.store_id
    JOIN silver_pg_products p     ON oi.product_id = p.product_id
    LEFT JOIN silver_promotions pr
           ON o.order_date BETWEEN pr.start_date AND pr.end_date
          AND pr.category = p.category
    GROUP BY o.store_id, s.name, s.region, s.surface_m2, s.store_type,
             o.order_date, o.channel, p.category, p.subcategory, pr.promo_name
""")

gold_sales.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true").saveAsTable("gold_sales_daily")

cnt = spark.table("gold_sales_daily").count()
print(f"✓ Cell 5 done — gold_sales_daily: {cnt:,} rows")

spark.sql("""
    SELECT YEAR(sales_date) AS yr, ROUND(SUM(revenue)/1e6,1) AS rev_M,
           ROUND(AVG(gross_margin/NULLIF(revenue,0))*100,1) AS margin_pct
    FROM gold_sales_daily GROUP BY yr ORDER BY yr
""").show()

# ── Cell 6 — Build gold_inventory_status ─────────────────────
# Regenerated from scratch with:
#   - English status values: stockout/critical/low/normal
#   - Realistic distribution (3/8/12/77%)
#   - coverage_days from actual sales velocity

stores_pd   = spark.table("silver_pg_stores").toPandas()
products_pd = spark.table("silver_pg_products").toPandas()

avg_daily_pd = spark.sql("""
    SELECT o.store_id, oi.product_id,
           SUM(oi.quantity) / COUNT(DISTINCT o.order_date) AS avg_daily_sales
    FROM silver_pg_order_items oi
    JOIN silver_pg_orders o ON o.order_id = oi.order_id
    GROUP BY o.store_id, oi.product_id
""").toPandas()
avg_idx = avg_daily_pd.set_index(["store_id","product_id"])["avg_daily_sales"].to_dict()

now = datetime.now()
rows = []
inv_id = 1

for _, store in stores_pd.iterrows():
    n     = int(len(products_pd) * random.uniform(0.60, 0.85))
    surf_f= math.log(store["surface_m2"] / 400 + 1)
    for _, prod in products_pd.sample(n, random_state=inv_id).iterrows():
        if not prod["is_active"]: continue
        adp           = max(0.1, float(avg_idx.get(
                           (int(store["store_id"]),int(prod["product_id"])), 1.0)))
        base          = max(5, int(adp * 14 * surf_f))
        safety_stock  = max(1, int(adp * 3))
        reorder_point = max(2, int(adp * 7))
        rnd = random.random()
        if rnd < 0.03:   qty,st = 0,"stockout"
        elif rnd < 0.11: qty,st = random.randint(1,safety_stock),"critical"
        elif rnd < 0.23: qty,st = random.randint(safety_stock+1,max(safety_stock+2,reorder_point)),"low"
        else:            qty,st = random.randint(reorder_point+1,base*2),"normal"
        cov = round(qty/adp,1) if adp>0 and qty>0 else (0.0 if qty==0 else None)
        rows.append({
            "inv_id":inv_id,"store_id":int(store["store_id"]),
            "store_name":store["name"],"region":store["region"],
            "product_id":int(prod["product_id"]),"product_name":prod["name"],
            "category":prod["category"],"quantity_on_hand":qty,
            "safety_stock":safety_stock,"reorder_point":reorder_point,
            "stock_status":st,"coverage_days":cov,
            "last_updated":now-timedelta(hours=random.randint(0,48)),
        })
        inv_id += 1

spark.createDataFrame(pd.DataFrame(rows)).write.format("delta").mode("overwrite") \
    .option("overwriteSchema","true").saveAsTable("gold_inventory_status")

spark.sql("""
    SELECT stock_status, COUNT(*) AS cnt,
           ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER(),1) AS pct,
           ROUND(AVG(coverage_days),1) AS avg_coverage
    FROM gold_inventory_status GROUP BY stock_status ORDER BY cnt DESC
""").show()
print("✓ Cell 6 done — gold_inventory_status with English status values")

# ── Cell 7 — Build gold_forecast_vs_actual ───────────────────

try:
    gold_fc = spark.sql("""
        SELECT f.product_id, p.name AS product_name, p.category,
               f.forecast_date,
               COALESCE(actual.actual_qty, 0) AS actual_qty,
               f.forecast_qty,
               f.forecast_qty_lower,
               f.forecast_qty_upper,
               CASE WHEN f.forecast_qty = 0 THEN NULL
                    ELSE ROUND(ABS(f.forecast_qty - COALESCE(actual.actual_qty,0))
                         / f.forecast_qty, 4) END AS forecast_error_pct,
               CASE WHEN f.forecast_qty = 0 THEN NULL
                    ELSE ROUND(1 - ABS(f.forecast_qty - COALESCE(actual.actual_qty,0))
                         / f.forecast_qty, 4) END AS forecast_accuracy
        FROM silver_adx_demand_forecast f
        JOIN silver_pg_products p ON CAST(p.product_id AS BIGINT) = f.product_id
        LEFT JOIN (
            SELECT oi.product_id, o.order_date, SUM(oi.quantity) AS actual_qty
            FROM silver_pg_order_items oi
            JOIN silver_pg_orders o ON o.order_id = oi.order_id
            GROUP BY oi.product_id, o.order_date
        ) actual ON actual.product_id = f.product_id
               AND actual.order_date  = f.forecast_date
    """)
    for field in gold_fc.schema.fields:
        if str(field.dataType).startswith("Decimal"):
            gold_fc = gold_fc.withColumn(field.name, F.col(field.name).cast(DoubleType()))
    gold_fc.write.format("delta").mode("overwrite") \
        .option("overwriteSchema","true").saveAsTable("gold_forecast_vs_actual")
    print(f"✓ Cell 7 done — gold_forecast_vs_actual: {gold_fc.count():,} rows")
except Exception as e:
    print(f"  SKIP gold_forecast_vs_actual — Databricks tables not yet available: {e}")

# ── Cell 8 — Cast DECIMAL → DOUBLE (Fabric IQ requirement) ───

tables_to_fix = [
    "gold_inventory_status","gold_sales_daily","gold_forecast_vs_actual",
    "silver_competitor_prices","silver_promotions",
]
for tbl in tables_to_fix:
    try:
        df = spark.table(tbl)
        dec = [f.name for f in df.schema.fields if str(f.dataType).startswith("Decimal")]
        if dec:
            print(f"  {tbl}: casting {dec}")
            for c in dec: df = df.withColumn(c, F.col(c).cast(DoubleType()))
            df.write.format("delta").mode("overwrite") \
              .option("overwriteSchema","true").saveAsTable(tbl)
    except Exception as e:
        print(f"  {tbl}: SKIP — {e}")
print("✓ Cell 8 done — DECIMAL → DOUBLE cast complete")

# ── Cell 9 — Pre-computed Gold tables ────────────────────────

# gold_double_risk_products
# Products in stockout/critical AND competitor is cheaper
try:
    dr = spark.sql("""
        SELECT DISTINCT
            p.product_id, p.name AS product_name, p.category,
            p.unit_price AS our_price,
            i.store_id, i.store_name, i.region,
            i.stock_status, i.coverage_days, i.quantity_on_hand,
            c.competitor AS cheapest_competitor,
            c.competitor_price,
            ROUND(c.price_gap_pct, 2) AS price_gap_pct
        FROM silver_pg_products p
        JOIN gold_inventory_status i
            ON i.product_id = p.product_id
           AND i.stock_status IN ('stockout','critical')
        JOIN silver_competitor_prices c
            ON CAST(c.product_id AS BIGINT) = CAST(p.product_id AS BIGINT)
           AND c.is_cheaper = true
           AND c.snapshot_month = (SELECT MAX(snapshot_month)
                                   FROM silver_competitor_prices)
        ORDER BY i.stock_status ASC, price_gap_pct ASC
    """)
    dr.write.format("delta").mode("overwrite") \
      .option("overwriteSchema","true").saveAsTable("gold_double_risk_products")
    print(f"✓ gold_double_risk_products: {dr.count():,} rows")
except Exception as e:
    print(f"  SKIP gold_double_risk_products: {e}")

# gold_champion_stockout_alert
# Champions who recently bought products now in stockout
try:
    cs = spark.sql("""
        SELECT DISTINCT
            seg.customer_id,
            seg.segment_label AS customer_segment,
            CAST(seg.monetary AS DOUBLE) AS lifetime_value,
            seg.recency_days, seg.frequency,
            p.product_id, p.name AS product_name, p.category,
            o.store_id, s.name AS store_name, s.region,
            i.stock_status,
            ROUND(i.coverage_days, 1) AS coverage_days,
            i.quantity_on_hand
        FROM silver_adx_customer_segments seg
        JOIN silver_pg_orders o      ON o.customer_id  = seg.customer_id
        JOIN silver_pg_order_items oi ON oi.order_id   = o.order_id
        JOIN silver_pg_products p    ON p.product_id   = oi.product_id
        JOIN silver_pg_stores s      ON s.store_id     = o.store_id
        JOIN gold_inventory_status i
            ON i.product_id = oi.product_id
        WHERE seg.segment_label = 'Champions'
          AND i.stock_status IN ('stockout','critical')
    """)
    cs.write.format("delta").mode("overwrite") \
      .option("overwriteSchema","true").saveAsTable("gold_champion_stockout_alert")
    print(f"✓ gold_champion_stockout_alert: {cs.count():,} rows")
except Exception as e:
    print(f"  SKIP gold_champion_stockout_alert — needs Databricks segments: {e}")

# ── Cell 10 — Dimension tables ───────────────────────────────
# Must be created as physical Delta tables — cannot use DAX
# calculated tables with Direct Lake sources

# dim_calendar with year_month_sort for correct chronological sort
spark.sql("""
    SELECT
        CAST(date AS DATE)                                AS date,
        DATE_FORMAT(date, 'yyyy-MM-dd')                   AS date_key,
        YEAR(date)                                        AS year,
        MONTH(date)                                       AS month_num,
        DATE_FORMAT(date, 'MMMM')                         AS month_name,
        CONCAT('Q', QUARTER(date))                        AS quarter,
        CONCAT(YEAR(date),'-',LPAD(MONTH(date),2,'0'))    AS year_month,
        YEAR(date) * 100 + MONTH(date)                    AS year_month_sort,
        WEEKOFYEAR(date)                                  AS week_num,
        CASE WHEN DAYOFWEEK(date) IN (1,7) THEN true
             ELSE false END                               AS is_weekend
    FROM (
        SELECT EXPLODE(SEQUENCE(
            TO_DATE('2022-01-01'),
            TO_DATE('2026-12-31'),
            INTERVAL 1 DAY
        )) AS date
    ) ORDER BY date
""").write.format("delta").mode("overwrite").saveAsTable("dim_calendar")
print("✓ dim_calendar")

spark.sql("SELECT DISTINCT category FROM silver_pg_products ORDER BY 1") \
    .write.format("delta").mode("overwrite").saveAsTable("dim_category")
print("✓ dim_category")

spark.sql("SELECT DISTINCT region FROM silver_pg_stores ORDER BY 1") \
    .write.format("delta").mode("overwrite").saveAsTable("dim_region")
print("✓ dim_region")

spark.sql("SELECT DISTINCT channel FROM silver_pg_orders ORDER BY 1") \
    .write.format("delta").mode("overwrite").saveAsTable("dim_channel")
print("✓ dim_channel")

# ── Cell 11 — Final validation ───────────────────────────────

print("\n" + "="*55)
print("GOLD LAYER — FINAL VALIDATION")
print("="*55)

gold_tables = [
    "gold_sales_daily","gold_inventory_status",
    "gold_forecast_vs_actual","gold_double_risk_products",
    "gold_champion_stockout_alert",
    "dim_calendar","dim_category","dim_region","dim_channel",
]
for t in gold_tables:
    try:
        cnt = spark.table(t).count()
        print(f"  ✓ {t}: {cnt:,} rows")
    except:
        print(f"  ✗ {t}: NOT FOUND")

print("="*55)
print("Next: Refresh retail360_sm Semantic Model")
print("="*55)
