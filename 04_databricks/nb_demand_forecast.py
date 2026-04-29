"""
Retail360 — nb_demand_forecast (Databricks)
============================================
Run in Azure Databricks. Uses INSERT OVERWRITE.

Prerequisites:
  %pip install prophet
  dbutils.library.restartPython()

Output: retail360_catalog.silver.demand_forecast
  Mirrored to Fabric as silver_adx_demand_forecast
"""

# Cell 1 — Install
# %pip install prophet
# dbutils.library.restartPython()

# Cell 2 — Imports
from prophet import Prophet
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

ONELAKE_PATH = (
    "abfss://<workspace-id>"
    "@onelake.dfs.fabric.microsoft.com"
    "/<lakehouse-id>/Tables/"
)

# Cell 3 — Read source
try:
    orders_df = spark.table("retail360_catalog.bronze.orders")
    items_df  = spark.table("retail360_catalog.bronze.order_items")
except:
    orders_df = spark.read.format("delta").load(ONELAKE_PATH + "silver_pg_orders")
    items_df  = spark.read.format("delta").load(ONELAKE_PATH + "silver_pg_order_items")

# Cell 4 — Daily sales by product
sales = (
    orders_df.join(items_df, "order_id")
    .groupBy("product_id", "order_date")
    .agg(F.sum("quantity").alias("y"))
    .withColumnRenamed("order_date", "ds")
    .toPandas()
)
sales["ds"] = pd.to_datetime(sales["ds"])
sales["y"]  = sales["y"].clip(lower=0).astype(float)

# Cell 5 — Top 200 SKUs by volume
top_skus = sales.groupby("product_id")["y"].sum().nlargest(200).index.tolist()
print(f"Forecasting {len(top_skus)} SKUs")

# Cell 6 — Prophet forecasting
forecasts = []
for i, sku in enumerate(top_skus):
    df_sku = sales[sales["product_id"] == sku][["ds","y"]].copy()
    if len(df_sku) < 30:
        continue
    try:
        m = Prophet(yearly_seasonality=True, weekly_seasonality=True,
                    changepoint_prior_scale=0.05, interval_width=0.80)
        m.fit(df_sku)
        future   = m.make_future_dataframe(periods=90, freq="D")
        forecast = m.predict(future).tail(90)[
            ["ds","yhat","yhat_lower","yhat_upper"]].copy()
        forecast["product_id"]  = sku
        forecast["yhat"]        = forecast["yhat"].clip(lower=0).round(2)
        forecast["yhat_lower"]  = forecast["yhat_lower"].clip(lower=0).round(2)
        forecast["yhat_upper"]  = forecast["yhat_upper"].clip(lower=0).round(2)
        forecasts.append(forecast)
        if i % 50 == 0:
            print(f"  {i}/{len(top_skus)} done")
    except Exception as e:
        print(f"  SKU {sku} skipped: {e}")

# Cell 7 — Write using INSERT OVERWRITE
result_df = pd.concat(forecasts).rename(columns={
    "ds": "forecast_date", "yhat": "forecast_qty",
    "yhat_lower": "forecast_qty_lower", "yhat_upper": "forecast_qty_upper"
})

forecast_spark = spark.createDataFrame(result_df)
for f in forecast_spark.schema.fields:
    if str(f.dataType).startswith("Decimal"):
        forecast_spark = forecast_spark.withColumn(
            f.name, F.col(f.name).cast(DoubleType()))

forecast_spark.createOrReplaceTempView("forecast_tmp")
spark.sql("""
    INSERT OVERWRITE retail360_catalog.silver.demand_forecast
    SELECT * FROM forecast_tmp
""")

print(f"✓ Written: {spark.table('retail360_catalog.silver.demand_forecast').count():,} rows")
