"""
Retail360 — nb_customer_segmentation (Databricks)
===================================================
Run in Azure Databricks. Uses INSERT OVERWRITE to preserve
Fabric Mirroring connection — never DROP TABLE.

Prerequisites:
  %pip install scikit-learn
  dbutils.library.restartPython()

Output: retail360_catalog.silver.customer_segments
  Mirrored to Fabric as silver_adx_customer_segments
"""

# Cell 1 — Install
# %pip install scikit-learn
# dbutils.library.restartPython()

# Cell 2 — Imports
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import pandas as pd
import numpy as np
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

REFERENCE_DATE = "2026-01-01"  # end of data range

# Cell 3 — Read source data
# Try Unity Catalog Bronze first, fall back to OneLake
ONELAKE_PATH = (
    "abfss://<workspace-id>"
    "@onelake.dfs.fabric.microsoft.com"
    "/<lakehouse-id>/Tables/"
)
try:
    orders_df = spark.table("retail360_catalog.bronze.orders")
    items_df  = spark.table("retail360_catalog.bronze.order_items")
    print("Reading from Unity Catalog Bronze")
except:
    orders_df = spark.read.format("delta").load(ONELAKE_PATH + "silver_pg_orders")
    items_df  = spark.read.format("delta").load(ONELAKE_PATH + "silver_pg_order_items")
    print("Reading from OneLake")

# Cell 4 — Compute RFM features
rfm = (
    orders_df.join(items_df, "order_id")
    .groupBy("customer_id")
    .agg(
        F.max("order_date").alias("last_order_date"),
        F.countDistinct("order_id").alias("frequency"),
        F.sum("line_total").alias("monetary"),
        F.avg("line_total").alias("avg_basket"),
        F.countDistinct("product_id").alias("nb_distinct_products"),
    )
    .withColumn("recency_days",
        F.datediff(F.to_date(F.lit(REFERENCE_DATE)),
                   F.col("last_order_date").cast("date")))
    .toPandas()
)
for col in ["monetary", "avg_basket"]:
    rfm[col] = rfm[col].astype(float)

print(f"Customers: {len(rfm):,}")

# Cell 5 — K-Means clustering
features = ["recency_days","frequency","monetary","avg_basket","nb_distinct_products"]
X_raw = rfm[features].fillna(0).astype(float)
for c in features:
    X_raw[c] = X_raw[c].clip(upper=float(X_raw[c].quantile(0.99)))

X = StandardScaler().fit_transform(X_raw)
rfm["segment_id"] = KMeans(n_clusters=5, random_state=42, n_init=10).fit_predict(X)

# Cell 6 — Assign English labels
profiles = rfm.groupby("segment_id")[features].mean()
profiles["score"] = (
    - profiles["recency_days"] / profiles["recency_days"].max()
    + profiles["frequency"]    / profiles["frequency"].max()
    + profiles["monetary"]     / profiles["monetary"].max()
)
ranking = profiles["score"].rank(ascending=False).astype(int)
label_map = {1:"Champions",2:"Loyal",3:"Warm Prospects",4:"At Risk",5:"Inactive"}
rfm["segment_label"] = rfm["segment_id"].map(
    {sid: label_map[r] for sid, r in ranking.items()})

# Verify English labels
assert set(rfm["segment_label"].unique()) == set(label_map.values()), \
    f"Unexpected labels: {rfm['segment_label'].unique()}"
print("✓ All segment labels are English")

# Cell 7 — Write using INSERT OVERWRITE (preserves Mirroring connection)
output = rfm[["customer_id","last_order_date","recency_days","frequency",
              "monetary","avg_basket","nb_distinct_products",
              "segment_id","segment_label"]].copy()

segments_spark = spark.createDataFrame(output)

# Cast DECIMAL to DOUBLE (Fabric IQ requirement)
for f in segments_spark.schema.fields:
    if str(f.dataType).startswith("Decimal"):
        segments_spark = segments_spark.withColumn(
            f.name, F.col(f.name).cast(DoubleType()))

# INSERT OVERWRITE — never DROP TABLE
segments_spark.createOrReplaceTempView("segments_tmp")
spark.sql("""
    INSERT OVERWRITE retail360_catalog.silver.customer_segments
    SELECT * FROM segments_tmp
""")

check = spark.table("retail360_catalog.silver.customer_segments")
print(f"✓ Written: {check.count():,} rows")
check.groupBy("segment_label").count().orderBy("count", ascending=False).show()
