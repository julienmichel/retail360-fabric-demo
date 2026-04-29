# Retail360 — Complete Setup Guide

Step-by-step guide to build the Retail360 Microsoft Fabric demo from scratch.
Incorporates all fixes and decisions made during the initial build session.

**Estimated time:** 4–6 hours first setup, 2–3 hours with scripts.

---

## Table of contents

1. [Architecture overview](#1-architecture-overview)
2. [Prerequisites](#2-prerequisites)
3. [Phase 1 — Repository and local setup](#3-phase-1--repository-and-local-setup)
4. [Phase 2 — Azure PostgreSQL](#4-phase-2--azure-postgresql)
5. [Phase 3 — Fabric workspace and Lakehouse](#5-phase-3--fabric-workspace-and-lakehouse)
6. [Phase 4 — PostgreSQL Mirroring](#6-phase-4--postgresql-mirroring)
7. [Phase 5 — Databricks ML notebooks](#7-phase-5--databricks-ml-notebooks)
8. [Phase 6 — Databricks Mirroring to Fabric](#8-phase-6--databricks-mirroring-to-fabric)
9. [Phase 7 — Dataflows Gen2 (external files)](#9-phase-7--dataflows-gen2-external-files)
10. [Phase 8 — Gold layer (Fabric notebooks)](#10-phase-8--gold-layer-fabric-notebooks)
11. [Phase 9 — Fabric SQL Database](#11-phase-9--fabric-sql-database)
12. [Phase 10 — Semantic Model](#12-phase-10--semantic-model)
13. [Phase 11 — Fabric IQ Ontology](#13-phase-11--fabric-iq-ontology)
14. [Phase 12 — Data Agent](#14-phase-12--data-agent)
15. [Phase 13 — Power BI Report](#15-phase-13--power-bi-report)
16. [Demo walkthrough script](#16-demo-walkthrough-script)
17. [Known limitations](#17-known-limitations)
18. [Troubleshooting](#18-troubleshooting)

---

## 1. Architecture overview

```
Azure PostgreSQL (source of truth)
  stores / products / orders / order_items / inventory
         │
         ▼  Fabric Mirroring (CDC)
  OneLake silver_pg_* tables
         │
         │    External CSV/JSON files (uploaded to Lakehouse Files/)
         │    weather / promotions / competitor_prices
         │         │
         │         ▼  Dataflow Gen2
         │    silver_weather / silver_promotions / silver_competitor_prices
         │
         │    Azure Databricks (Prophet + K-Means ML)
         │    Unity Catalog: silver.demand_forecast
         │                   silver.customer_segments
         │         │
         │         ▼  Databricks Mirroring
         │    silver_adx_demand_forecast / silver_adx_customer_segments
         │
         ▼  Spark Notebooks (Fabric)
  Gold layer (Delta, Direct Lake)
    gold_sales_daily
    gold_inventory_status           (coverage_days, English status values)
    gold_forecast_vs_actual
    gold_double_risk_products       (pre-computed: stockout + competitor cheaper)
    gold_champion_stockout_alert    (pre-computed: Champion customers + stockout)
    dim_calendar / dim_category / dim_region / dim_channel
         │
         ├──► retail360_sm  (Semantic Model, Direct Lake)
         │         │
         │         ├──► Power BI Report (4 pages + drill-through)
         │         └──► Copilot
         │
         ├──► retail360_om  (Fabric IQ Ontology)
         │         │
         │         └──► retail360_data_agent
         │                  └── also connected to retail360_sm + retail360_sql
         │
         └──► retail360_sql  (Fabric SQL Database — operational only)
                  transfer_orders / customer_offers / active_alerts / incidents
```

---

## 2. Prerequisites

### Azure resources
- Azure subscription with Contributor access
- **Azure Database for PostgreSQL Flexible Server** (General Purpose, 4 vCores+)
- **Microsoft Fabric capacity** F8 minimum
- **Azure Databricks workspace** Premium tier (required for Unity Catalog)
- Azure AD App Registration (Service Principal) for REST API scripts

### Local tools
- Python 3.9+
- VS Code with PostgreSQL extension
- Git

### Permissions
- Fabric workspace Admin
- Databricks Admin (to create Unity Catalog)
- PostgreSQL admin user

> **Note on Oracle:** The original design used Oracle. During the build session this was changed to **Azure PostgreSQL** because it supports native Fabric Mirroring without requiring a gateway. PostgreSQL also has better Python tooling for data generation.

---

## 3. Phase 1 — Repository and local setup

```bash
git clone https://github.com/your-org/retail360-fabric-demo
cd retail360-fabric-demo
pip install -r config/requirements.txt
cp config/config.example.json config/config.json
```

Edit `config/config.json` — fill in all values before running any script:

```json
{
  "postgresql": {
    "host": "retail360.postgres.database.azure.com",
    "port": 5432,
    "dbname": "retail360",
    "user": "retail360admin",
    "password": "your-password",
    "sslmode": "require"
  },
  "fabric": {
    "workspace_name": "Retail360-Demo",
    "capacity_id": "your-capacity-id",
    "client_id": "your-service-principal-client-id",
    "client_secret": "your-service-principal-secret"
  },
  "azure": {
    "tenant_id": "your-tenant-id"
  },
  "databricks": {
    "workspace_url": "https://adb-xxxx.azuredatabricks.net",
    "token": "your-pat-token",
    "catalog": "retail360_catalog"
  }
}
```

---

## 4. Phase 2 — Azure PostgreSQL

### Step 1 — Create Flexible Server

Azure Portal → **Azure Database for PostgreSQL Flexible Server** → Create:

```
Compute    : General Purpose, Standard_D4s_v3 (4 vCores)
Storage    : 128 GB SSD
PostgreSQL : version 15+
Database   : retail360
```

### Step 2 — Enable logical replication (REQUIRED for Mirroring)

Server → **Server parameters** → search `azure.replication_support`:
- Set value to **LOGICAL** → Save

Verify:
```sql
SHOW wal_level;             -- must return 'logical'
SHOW azure.replication_support;  -- must return 'logical'
```

### Step 3 — Configure firewall

Server → **Networking**:
- ✓ Allow public access from Azure services
- Add your local IP for script access

### Step 4 — Generate data

```bash
python 01_postgresql/generate_data.py
```

Duration: ~15–25 minutes for 2M+ rows.

**What is generated:**

| Table | Rows | Notes |
|---|---|---|
| stores | 150 | 9 French regions, 3 store formats |
| products | 500 | 10 categories, realistic margins |
| orders | ~600,000 | 2022–2026, seasonal patterns |
| order_items | ~2,000,000 | Pareto product distribution |
| inventory | ~40,000 | English status values: stockout/critical/low/normal |

**External files in `./output_files/`:**
- `weather/YYYY-MM-DD.json` — daily weather per city
- `promotions/promo_calendar.csv` — 7 events per year in English
- `competitor_prices/competitor_prices_YYYY_MM.csv` — monthly pricing

### Step 5 — Fix labels to English (IMPORTANT)

The data generator outputs English labels by default, but if migrating from an older version that used French labels, run:

```bash
# Connect via VS Code PostgreSQL extension then run:
01_postgresql/fix_labels.sql
```

This fixes:
- `store_type`: hypermarché → hypermarket, supermarché → supermarket, proximité → convenience
- `channel`: magasin → in_store, livraison_domicile → home_delivery

> **Why this matters:** Fabric Mirroring uses CDC — any UPDATE in PostgreSQL propagates automatically within ~5 minutes. The Fabric notebooks must NEVER directly edit mirrored tables.

### Step 6 — Verify data

```bash
# Run in VS Code PostgreSQL
01_postgresql/verify_data.sql
```

Expected:
```
store_type: hypermarket / supermarket / convenience    ✓
channel   : in_store / click_and_collect /
            home_delivery / drive                      ✓
stock_status: stockout / critical / low / normal       ✓
```

---

## 5. Phase 3 — Fabric workspace and Lakehouse

### Step 7 — Create workspace

```bash
python 02_fabric_workspace/create_workspace.py
```

Creates:
- Workspace `Retail360-Demo` on your F64 capacity
- Lakehouse `retail360_lh`
- SQL Database `retail360_sql`

Writes generated IDs back to `config/config.json`.

### Step 8 — Create Lakehouse folder structure

In `retail360_lh` → **Files** section → create:

```
Files/
  raw/
    weather/
    promotions/
    competitor_prices/
```

### Step 9 — Upload external files

Upload `./output_files/` contents to the Lakehouse:

```
output_files/weather/             → Files/raw/weather/
output_files/promotions/          → Files/raw/promotions/
output_files/competitor_prices/   → Files/raw/competitor_prices/
```

Use Fabric portal drag-and-drop or OneLake Explorer for large file sets.

---

## 6. Phase 4 — PostgreSQL Mirroring

### Step 10 — Create Mirrored Database

Workspace → **+ New item** → **Mirrored Database** → **Azure Database for PostgreSQL**

Connection:
```
Server   : retail360.postgres.database.azure.com
Port     : 5432
Database : retail360
User     : retail360admin
Password : your-password
SSL      : Required
```

Select tables:
- ✓ stores
- ✓ products
- ✓ orders
- ✓ order_items
- ✓ inventory

Click **Mirror database**. Initial snapshot takes 10–20 minutes.

### Step 11 — Create Lakehouse shortcuts

In `retail360_lh` → **Tables** → **...** → **New shortcut** → **Microsoft OneLake**

Navigate to the Mirrored Database → select all 5 tables → **Create**.

Tables appear as:
- `silver_pg_stores`
- `silver_pg_products`
- `silver_pg_orders`
- `silver_pg_order_items`
- `silver_pg_inventory`

> **Important:** These are read-only shortcuts. NEVER write directly to these tables from Fabric notebooks — always make changes at the PostgreSQL source.

---

## 7. Phase 5 — Databricks ML notebooks

### Step 12 — Create Unity Catalog schemas

In a Databricks notebook:

```sql
CREATE CATALOG IF NOT EXISTS retail360_catalog;
CREATE SCHEMA IF NOT EXISTS retail360_catalog.bronze;
CREATE SCHEMA IF NOT EXISTS retail360_catalog.silver;
```

### Step 13 — Configure OneLake access

In your Databricks cluster → **Configuration** → add MSI authentication to the OneLake storage account.

In both Databricks notebooks, update `ONELAKE_PATH`:
```python
ONELAKE_PATH = (
    "abfss://<workspace-id>"
    "@onelake.dfs.fabric.microsoft.com"
    "/<lakehouse-id>/Tables/"
)
```

Replace `<workspace-id>` and `<lakehouse-id>` from your `config/config.json`.

### Step 14 — Run demand forecast notebook

Upload and run `04_databricks/nb_demand_forecast.py`.

```python
# Install on cluster first:
%pip install prophet
```

Duration: ~20–40 minutes for 200 SKUs.
Output: `retail360_catalog.silver.demand_forecast`

### Step 15 — Run customer segmentation notebook

Upload and run `04_databricks/nb_customer_segmentation.py`.

```python
# Install on cluster first:
%pip install scikit-learn
```

Duration: ~5–10 minutes.
Output: `retail360_catalog.silver.customer_segments`

> **Critical pattern:** Both notebooks use `INSERT OVERWRITE` instead of `DROP TABLE + saveAsTable`. This preserves the table registration in Unity Catalog and keeps the Fabric Mirroring connection alive. Never use DROP TABLE on Databricks tables that are mirrored to Fabric.

**Reference date = 2026-01-01:** The segmentation notebook uses this as the recency reference date so that Champions have low recency_days (purchased recently relative to data end) and Inactive customers have high recency_days.

**English segment labels (verified):**
```
Champions / Loyal / Warm Prospects / At Risk / Inactive
```

### Step 16 — Grant permissions to Fabric service principal

After running notebooks, grant SELECT access so Mirroring can read the tables:

```sql
-- In Databricks notebook
GRANT SELECT ON TABLE retail360_catalog.silver.demand_forecast
TO `fabric-sp@yourtenant.com`;

GRANT SELECT ON TABLE retail360_catalog.silver.customer_segments
TO `fabric-sp@yourtenant.com`;

GRANT USE SCHEMA ON SCHEMA retail360_catalog.silver
TO `fabric-sp@yourtenant.com`;

GRANT USE CATALOG ON CATALOG retail360_catalog
TO `fabric-sp@yourtenant.com`;
```

Replace `fabric-sp@yourtenant.com` with the service principal from your Databricks Mirroring connection settings.

---

## 8. Phase 6 — Databricks Mirroring to Fabric

### Step 17 — Mirror Databricks Unity Catalog

Workspace → **+ New item** → **Mirrored Database** → **Azure Databricks**

```
Workspace URL : https://adb-xxxx.azuredatabricks.net
Access token  : your-PAT-token
Catalog       : retail360_catalog
```

Select:
- ✓ silver.demand_forecast
- ✓ silver.customer_segments

### Step 18 — Create shortcuts in Lakehouse

In `retail360_lh` → **Tables** → **New shortcut** → **Microsoft OneLake** → navigate to Databricks Mirrored Database → select both tables.

Tables appear as:
- `silver_adx_demand_forecast`
- `silver_adx_customer_segments`

---

## 9. Phase 7 — Dataflows Gen2 (external files)

### Step 19 — Create Dataflow Gen2

Workspace → **+ New item** → **Dataflow Gen2** → name: `df_files_ingestion`

For each of the 3 queries, open **Advanced editor** and paste the M code from `05_dataflows/`. Replace `<workspace-id>` and `<lakehouse-id>` with your GUIDs from the Fabric URL.

**qry_weather** (from `05_dataflows/qry_weather.pq`)
- Destination: `retail360_lh` → `silver_weather` → **Append** mode

**qry_promotions** (from `05_dataflows/qry_promotions.pq`)
- Destination: `retail360_lh` → `silver_promotions` → **Replace** mode

**qry_competitor** (from `05_dataflows/qry_competitor.pq`)
- Destination: `retail360_lh` → `silver_competitor_prices` → **Append** mode

Click **Publish** → run the dataflow → verify all 3 tables are populated.

Expected row counts:
```
silver_weather              : ~78,000 rows
silver_promotions           : ~159 rows
silver_competitor_prices    : ~36,750 rows
```

---

## 10. Phase 8 — Gold layer (Fabric notebooks)

### Step 20 — Run the Silver-to-Gold notebook

In `retail360_lh` → **Open notebook** → paste content of `03_lakehouse/nb_silver_to_gold.py`.

Run all cells in order. The notebook performs the following in sequence:

**Cell 1 — Verify sources exist**
Checks all required Silver tables are accessible before proceeding.

**Cell 2 — Fix silver_weather column names**

Renames columns to match ontology property names:
```
precipitation_mm → rainfall_mm
temp_max_c       → max_temperature_c
temp_min_c       → min_temperature_c
wind_kmh         → wind_speed_kmh
weather_label    → weather_condition
```

> This rename is essential. The ontology property names and the physical column names must match exactly or the GQL engine returns "property not found" errors.

**Cell 3 — Fix competitor pricing variance**

Regenerates `price_gap_pct` with realistic competitor positioning:
```
MaxiMarché    : bias -5%  (cheaper on average)
PrimoShop     : bias +3%  (more expensive)
BonPrix       : bias -10% (aggressive discounter)
NeoShop       : bias +2%  (slightly more expensive)
TendancePlus  : bias -2%  (slightly cheaper)
```

**Cell 4 — Fix promo names to English**
```
Soldes Hiver          → Winter Sales
Soldes Été            → Summer Sales
Rentrée scolaire      → Back to School
Saint-Valentin        → Valentine's Day
Fêtes de fin d'année  → Christmas
```

**Cell 5 — Build gold_sales_daily**

Aggregated from `silver_pg_orders + silver_pg_order_items + silver_pg_stores + silver_pg_products + silver_promotions`. Includes `revenue`, `gross_margin`, `nb_orders`, `units_sold`, `avg_discount_pct`, `is_promo_day`, `promo_name`. No `product_id` column — aggregated at store/category/date/channel level.

**Cell 6 — Build gold_inventory_status (CRITICAL)**

Regenerated from scratch with:
- Correct English status values: `stockout / critical / low / normal`
- Realistic distribution: ~3% stockout / ~8% critical / ~12% low / ~77% normal
- Thresholds based on actual avg daily sales per store+product
- `coverage_days` = quantity_on_hand / avg_daily_sales (computed from order_items)
- Uses `safety_stock = 3 days` and `reorder_point = 7 days` thresholds

> **Why this cell is critical:** The PostgreSQL `inventory` table is NOT used for the Gold layer. The Gold inventory is regenerated in Fabric using the actual sales velocity from order history, giving realistic and meaningful coverage_days values.

**Cell 7 — Build gold_forecast_vs_actual**

Joins `silver_adx_demand_forecast` with actual sales from `silver_pg_order_items`. Requires Databricks notebooks to have run first.

**Cell 8 — Cast DECIMAL → DOUBLE**

Required for Fabric IQ GQL compatibility. The GQL engine does not support DECIMAL aggregations. All numeric columns in Gold tables are cast to DOUBLE.

**Cell 9 — Build pre-computed Gold tables**

`gold_double_risk_products`:
- Products where `stock_status IN (stockout, critical)` AND `is_cheaper = true` in latest competitor snapshot
- Solves the Fabric IQ limitation of two simultaneous diverging relationship paths

`gold_champion_stockout_alert`:
- Champions who ordered products (last 90 days) that are now in stockout
- Solves the missing `Order → OrderItem` traversal path in the ontology

**Cell 10 — Build dimension tables**

Creates `dim_calendar`, `dim_category`, `dim_region`, `dim_channel` as physical Delta tables in the Lakehouse.

> **Why dimensions are built in Spark (not DAX):** Calculated DAX tables cannot reference Direct Lake tables in Fabric. Dimension tables must be created as physical Delta tables.

**Cell 11 — Final validation**

Expected row counts after full run:
```
gold_sales_daily              : ~1,500,000+
gold_inventory_status         : ~52,000
gold_forecast_vs_actual       : ~18,000
gold_double_risk_products     : ~5,000–15,000
gold_champion_stockout_alert  : varies
dim_calendar                  : 1,827
```

---

## 11. Phase 9 — Fabric SQL Database

### Step 21 — Create operational tables

In the workspace → `retail360_sql` → **New query** → paste and run:
```
08_sql_database/create_operational_tables.sql
```

Creates:
- `dbo.transfer_orders` — replenishment orders by status
- `dbo.customer_offers` — retention offers sent to customers
- `dbo.active_alerts` — stockout and anomaly alerts
- `dbo.incidents` — operational incidents

Seeds each table with 10 demo rows for the walkthrough.

---

## 12. Phase 10 — Semantic Model

### Step 22 — Create Semantic Model

In `retail360_lh` → **New semantic model** → name: `retail360_sm`

Select all these tables:
```
✓ gold_sales_daily              ✓ dim_calendar
✓ gold_inventory_status         ✓ dim_category
✓ gold_forecast_vs_actual       ✓ dim_region
✓ gold_double_risk_products     ✓ dim_channel
✓ gold_champion_stockout_alert  ✓ silver_pg_stores
✓ silver_adx_customer_segments  ✓ silver_pg_products
✓ silver_adx_demand_forecast
✓ silver_competitor_prices
✓ silver_weather
✓ silver_promotions
```

### Step 23 — Create relationships

In the Semantic Model → **Manage relationships** → create exactly these 9 relationships:

| From table | From column | To table | To column | Cardinality | Cross-filter |
|---|---|---|---|---|---|
| gold_sales_daily | sales_date | dim_calendar | date | Many-to-One | Single |
| gold_sales_daily | store_id | silver_pg_stores | store_id | Many-to-One | Single |
| gold_forecast_vs_actual | forecast_date | dim_calendar | date | Many-to-One | Single |
| gold_forecast_vs_actual | product_id | silver_pg_products | product_id | Many-to-One | Single |
| gold_inventory_status | store_id | silver_pg_stores | store_id | Many-to-One | Single |
| gold_inventory_status | product_id | silver_pg_products | product_id | Many-to-One | Single |
| silver_adx_demand_forecast | product_id | silver_pg_products | product_id | Many-to-One | Single |
| silver_competitor_prices | product_id | silver_pg_products | product_id | Many-to-One | Single |
| silver_weather | city | silver_pg_stores | city | Many-to-One | Single |

**Do NOT create relationships on:**
- `category` column — creates ambiguous paths between tables
- `region` column — already denormalized in Gold tables
- `channel` column — use column directly in visuals
- `silver_promotions` — standalone filter table, no join key
- `silver_adx_customer_segments` — standalone RFM table, no join key

### Step 24 — Create DAX measures

In the Semantic Model → select each table → **New measure** → paste from `06_semantic_model/measures.dax`.

Organize into display folders:
```
Ventes       : Total Revenue, Gross Margin %, Revenue MTD/QTD/YTD,
               Revenue vs LY, Nb Orders, Avg Basket, Revenue uplift promo
Inventaire   : Stockout Rate, Nb Produits rupture, Nb Produits critique,
               Stock jours couverture
Forecast     : Forecast Accuracy, MAE Forecast, Forecast 30j, Forecast 90j
Clients      : Nb Clients total, Nb Champions, % Champions,
               Monetary moyen Champions
Concurrence  : Avg Price Gap vs Competitors, % Produits moins chers
```

> **Key measure — Stockout Rate:** Uses `stock_status = "stockout"` (English). If you see 0% stockout rate, the Gold table still contains French values — rerun Cell 6 of the Silver-to-Gold notebook.

### Step 25 — Add sort key to dim_calendar

In the Semantic Model → select `dim_calendar` → click column `year_month` → **Column tools** → **Sort by column** → select `year_month_sort`.

If `year_month_sort` doesn't exist, add it in the Fabric notebook:

```python
from pyspark.sql import functions as F

dim_cal = spark.table("dim_calendar")
dim_cal.withColumn(
    "year_month_sort",
    F.year(F.col("date")) * 100 + F.month(F.col("date"))
).write.format("delta").mode("overwrite") \
 .option("overwriteSchema","true") \
 .saveAsTable("dim_calendar")
```

---

## 13. Phase 11 — Fabric IQ Ontology

### Step 26 — Create the Ontology item

Workspace → **+ New item** → **Ontology** → name: `retail360_om`

### Step 27 — Configure entity types

Create each entity type with the following configuration. For each entity:
1. Click **+ Add entity type**
2. Set the binding to `retail360_lh` → `dbo` → [table]
3. Set the Key property
4. Set the Instance display name
5. Add/rename properties using ontology names (not physical column names)

#### Store → silver_pg_stores
```
Key              : store_id
Display name     : name
Properties to add/rename:
  name           (keep)
  city           (keep)
  region         (keep)
  store_type   → Store_Format
  surface_m2   → Store_Size_m2
  opening_date   (keep)
  manager_name   (keep)
  phone          (keep)
```

#### Product → silver_pg_products
```
Key              : product_id
Display name     : name
Properties to add/rename:
  name           (keep)
  category     → Product_Category
  subcategory  → Product_Subcategory
  unit_price   → Selling_Price
  unit_cost    → Cost_Price
  is_active      (keep)
  launch_date    (keep)
  ean            (keep)
```

#### InventoryStatus → gold_inventory_status
```
Key              : store_id
Display name     : product_name
Properties to add/rename:
  store_id       (keep)
  store_name     (keep)
  region         (keep)
  product_id     (keep)
  product_name   (keep)
  category       (keep — NOT renamed, physical name used in GQL)
  quantity_on_hand → Stock_On_Hand
  safety_stock    → Safety_Stock_Threshold
  reorder_point   → Reorder_Point
  stock_status    → Stock_Health_Status
  last_updated    (keep)
  coverage_days → Coverage_Days  (ADD THIS — was added after initial setup)
```

> **Important on coverage_days:** This column was added to `gold_inventory_status` after the ontology was initially created. After adding the property, republish the ontology. The graph index caches data at publish time — see Known Limitations.

#### ForecastVsActual → gold_forecast_vs_actual
```
Key              : product_id
Display name     : product_name
Properties:
  product_id, product_name, category
  forecast_date → Forecast_Date
  actual_qty    → Actual_Quantity
  forecast_qty  → Forecasted_Quantity
  forecast_qty_lower → Forecast_Lower_Bound
  forecast_qty_upper → Forecast_Upper_Bound
  forecast_error_pct → Forecast_Error_Pct
  forecast_accuracy  → Forecast_Accuracy
```

#### DemandForecast → silver_adx_demand_forecast
```
Key              : product_id
Display name     : product_id
Properties:
  product_id
  forecast_date  → Forecast_Date
  forecast_qty   → Forecasted_Quantity
  forecast_qty_lower → Forecast_Lower_Bound
  forecast_qty_upper → Forecast_Upper_Bound
```

#### CustomerSegment → silver_adx_customer_segments
```
Key              : customer_id
Display name     : segment_label
Properties:
  customer_id
  last_order_date
  segment_label    → Customer_Segment
  recency_days     → Days_Since_Last_Purchase
  frequency        → Purchase_Frequency
  monetary         → Lifetime_Value
  avg_basket       → Average_Basket_Size
  nb_distinct_products → Distinct_Products_Bought
```

#### CompetitorPrice → silver_competitor_prices
```
Key              : product_id
Display name     : product_name
Properties:
  product_id, product_name, category
  snapshot_month
  competitor       → Competitor_Name
  competitor_price (keep)
  our_price        (keep)
  price_gap_pct    → Price_Gap_Pct
  is_cheaper       → Competitor_Is_Cheaper
  source           (keep)
```

#### WeatherObservation → silver_weather
```
Key              : city
Display name     : weather_condition
Properties:
  date, city, region
  max_temperature_c  → Max_Temperature_C
  min_temperature_c  → Min_Temperature_C
  rainfall_mm        → Rainfall_mm
  wind_speed_kmh     → Wind_Speed_kmh
  weather_condition  → Weather_Condition
  is_sunny           (keep)
```

> **Column name dependency:** The silver_weather table was renamed from `precipitation_mm` / `temp_max_c` etc. to `rainfall_mm` / `max_temperature_c` etc. in the Silver-to-Gold notebook Cell 2. The ontology binds to the renamed column names.

#### Promotion → silver_promotions
```
Key              : promo_id
Display name     : promo_name
Properties:
  promo_id
  promo_name
  start_date → Start_Date
  end_date   → End_Date
  category   → Product_Category
  discount_pct → Discount_Pct
  traffic_boost → Traffic_Boost_Factor
  budget_eur    → Budget_EUR
  is_active     → Is_Active
```

### Step 28 — Create relationships

For each relationship, click **+ Add relationship** on the source entity:

| Relationship name | Source entity | Source column | Target entity | Target column |
|---|---|---|---|---|
| holds_inventory | InventoryStatus | store_id | Store | store_id |
| located_in | InventoryStatus | store_id | Store | store_id |
| tracked_in_inventory | InventoryStatus | product_id | Product | product_id |
| has_product | InventoryStatus | product_id | Product | product_id |
| is_forecasted_for | ForecastVsActual | product_id | Product | product_id |
| benchmarked_against | CompetitorPrice | product_id | Product | product_id |
| in_demand_forecast | DemandForecast | product_id | Product | product_id |
| observed_at | Store | city | WeatherObservation | city |
| occurs_on (FVA) | ForecastVsActual | forecast_date | dim_calendar | date |
| starts_on | Promotion | start_date | dim_calendar | date |
| at_location | WeatherObservation | city | Store | city |

> **Confirmed GQL relationship directions** (validated during build session):
> ```
> InventoryStatus -[:tracked_in_inventory]→ Product    ✓
> InventoryStatus -[:located_in]→ Store                ✓
> CompetitorPrice -[:benchmarked_against]→ Product     ✓
> ForecastVsActual -[:is_forecasted_for]→ Product      ✓
> DemandForecast -[:in_demand_forecast]→ Product       ✓
> Store -[:observed_at]→ WeatherObservation            ✓
> ```

### Step 29 — Publish

Click **Publish** top right → wait for green confirmation.

> **Graph index caching note:** After publishing, the GQL query engine builds a graph index asynchronously. This can take minutes to hours. The binding preview reads Delta directly and shows correct values immediately. GQL queries use the index. If GQL returns stale values after a Gold table rebuild, see Known Limitations.

---

## 14. Phase 12 — Data Agent

### Step 30 — Create Data Agent

Workspace → **+ New item** → **Data agent** → name: `retail360_data_agent`

### Step 31 — Add data sources

**Add Data** → add all three:
1. `retail360_om` — Ontology
2. `retail360_sm` — Semantic Model
3. `retail360_sql` — Fabric SQL Database

### Step 32 — Configure Agent instructions

In **Setup → Agent instructions** → paste the complete content from:
```
07_fabric_iq/agent_instructions.txt
```

### Step 33 — Configure SQL examples for retail360_sql

In **Setup → retail360_sql** tab → **Example queries**, add:

```sql
-- How many transfer orders are pending?
SELECT status, COUNT(*) AS cnt, SUM(quantity) AS total_units
FROM dbo.transfer_orders
GROUP BY status
ORDER BY cnt DESC;
```

```sql
-- Active alerts by severity
SELECT alert_type, severity, COUNT(*) AS cnt
FROM dbo.active_alerts
WHERE status = 'active'
ORDER BY CASE severity
    WHEN 'critical' THEN 1
    WHEN 'high' THEN 2
    WHEN 'medium' THEN 3
    ELSE 4 END;
```

```sql
-- Customer offers sent this week
SELECT offer_type, COUNT(*) AS cnt,
       ROUND(AVG(discount_pct),1) AS avg_discount
FROM dbo.customer_offers
WHERE generated_at >= DATEADD(day,-7,GETDATE())
GROUP BY offer_type;
```

### Step 34 — Test the Data Agent

Validate each layer with these questions:

**Ontology — single entity:**
```
"Which stores are hypermarkets?"
"Which products are cheaper at BonPrix?"
"What is the forecast accuracy for Électronique products?"
```

**Semantic Model — KPIs:**
```
"What is our overall forecast accuracy?"
"How many products are currently in stockout?"
"Which are the top 5 stores by revenue in 2024?"
```

**Semantic Model — time intelligence:**
```
"Compare gross margin by product category 2024 vs 2023"
"What was our revenue in Q4 2024 vs Q4 2023?"
```

**Pre-computed Gold:**
```
"Which products are both out of stock AND cheaper at competitors?"
"Which Champion customers recently bought products now in stockout?"
```

**SQL Database:**
```
"How many transfer orders are pending right now?"
"Show me active critical alerts"
```

---

## 15. Phase 13 — Power BI Report

### Step 35 — Create report

Workspace → **+ New item** → **Report** → select `retail360_sm` → **Create**.

### Page 1 — Executive Overview

| Visual | Config |
|---|---|
| 5 KPI Cards (top row) | Total Revenue (€M), Revenue vs LY (%), Gross Margin %, Stockout Rate (conditional), Nb Orders |
| Line chart | X: dim_calendar[year_month] sorted by year_month_sort, Y1: Total Revenue, Y2: Revenue vs LY (secondary axis, format +0.0%) |
| Horizontal bar | Top 10 stores by Total Revenue |
| Filled map | Location: region, Size: Total Revenue, Color: Gross Margin % |
| Donut | Legend: Customer_Segment, Values: Nb Clients total |
| Slicers | dim_calendar[year], silver_pg_stores[region], gold_sales_daily[channel] |

> **Revenue format:** Apply format string `€#,##0,,.0"M"` on the Total Revenue measure to display millions.

> **Revenue vs LY secondary axis:** In the line chart → Values well → click dropdown on Revenue vs LY → Show on secondary axis. Format: `+0.0%;-0.0%;0.0%`

### Page 2 — Inventory Intelligence

| Visual | Config |
|---|---|
| 5 KPI Cards | Stockout Rate, Nb Produits rupture, Nb Produits critique, Stock moyen, Stock jours couverture |
| Matrix | Rows: category, Columns: region, Values: Stockouts Display + Stock jours couverture (Average). Conditional formatting on coverage days: <7=red, 7-14=orange, 14-20=yellow, 20-25=light green, >25=green |
| Scatter chart | X: MEDIAN(our_price), Y: MEDIAN(competitor_price), Legend: Product_Category, Size: remove or use fixed value |
| Table | gold_double_risk_products sorted by price_gap_pct ASC, filter snapshot_month = latest, no total row |

> **Matrix blank column fix:** Add visual-level filter on `region IS NOT NULL`.

> **Scatter X/Y axis:** Use Median not Average to avoid outlier distortion. Logarithmic scale helps spread low-price clusters.

### Page 3 — Forecast & ML

| Visual | Config |
|---|---|
| 4 KPI Cards | Forecast Accuracy, MAE Forecast, Forecast 30j, Forecast 90j |
| Line chart | X: dim_calendar[date], Y: Actual_Quantity (solid) + Forecasted_Quantity (dashed) + confidence band |
| Horizontal bar | Product_Category by Forecast Accuracy ASC (worst first), conditional formatting gradient red→green |
| Scatter | X: AVG(Max_Temperature_C), Y: Total Revenue, Legend: Weather_Condition, Size: Nb Orders |
| Area chart | X: dim_calendar[date] future only, Y: Forecasted_Quantity + bounds |

### Page 4 — Intelligence 360°

| Visual | Config |
|---|---|
| Horizontal bar | silver_promotions[promo_name] by Revenue uplift promo |
| Scatter bubble | X: Days_Since_Last_Purchase, Y: Lifetime_Value, Size: Nb Clients total, Legend: Customer_Segment |
| Matrix heatmap | Rows: Store_Format, Columns: Product_Category, Values: Stockout Rate, gradient white→red |
| Clustered bar | X: Product_Category, Y: AVG(Price_Gap_Pct), Legend: Competitor_Name, reference line at 0 |

### Page 5 — Store Detail (hidden, drill-through)

Set as drill-through page → drill-through field: `silver_pg_stores[name]`

Contains: Dynamic title, 5 KPI cards, revenue by category bar, monthly trend line, top 20 products table.

### Page 6 — Store Tooltip (hidden)

Set page size to Tooltip (320×240px) → Allow use as tooltip = ON

Assign to regional map and top stores bar chart.

---

## 16. Demo walkthrough script

**Duration: ~10 minutes**

```
STEP 1 — Power BI Page 1 (Executive Overview)
"This is Retail360 — 150 stores, 500 products, 4 years of data,
 all unified in Microsoft Fabric with no data movement."

STEP 2 — Power BI Page 2 (Inventory Intelligence)
"The inventory matrix shows stockout and coverage days by category
 and region. Green is healthy stock, red is urgent action needed."

STEP 3 — Data Agent Q1 (Ontology)
"Which stores in Île-de-France have products in critical
 or stockout status?"
→ Shows ontology relationship traversal: InventoryStatus → Store

STEP 4 — Data Agent Q2 (Semantic Model time intelligence)
"Compare our gross margin by product category for 2024 vs 2023
 and identify which categories are declining."
→ Shows SAMEPERIODLASTYEAR DAX intelligence

STEP 5 — Data Agent Q3 (Semantic Model cross-KPI)
"Which product categories have the worst forecast accuracy
 AND the highest competitor price pressure?"
→ Shows 2-step synthesis across two Semantic Model queries

STEP 6 — Data Agent Q4 (Pre-computed Gold)
"Which products are both out of stock AND cheaper at competitors?"
→ Shows gold_double_risk_products — ML + live inventory intersection

STEP 7 — Data Agent Q5 (Pre-computed Gold)
"Which Champion customers recently bought products now in stockout?
 Prioritize by lifetime value."
→ Shows gold_champion_stockout_alert — Databricks ML + PostgreSQL CDC

STEP 8 — Data Agent Q6 (SQL Database)
"How many transfer orders did our replenishment agent create
 this week and what is their current status?"
→ Shows full agentic loop: Reflex alert → agent creates order → report

STEP 9 — Power BI Copilot (bonus)
"Summarize the inventory situation in Grand Est this month"
→ Shows Copilot narrative generation from Semantic Model
```

---

## 17. Known limitations

### Fabric IQ GQL graph index caching

**Symptom:** GQL queries return stale data after Gold table rebuild. The binding preview shows correct values but GQL returns old values.

**Root cause:** The Fabric IQ graph index is built asynchronously at publish time and cached independently of the Delta table. The binding preview reads Delta directly (bypasses cache). GQL queries use the cached index.

**Current status:** Known Fabric IQ preview platform limitation. Tracked internally as graph model id `ef4b3727-9118-4936-bb4c-3c256dfb04fa`.

**Workaround:** Route all inventory status questions through the Semantic Model which reads Delta directly via Direct Lake. Use the ontology for entity lookups and relationship traversal only, not for `stock_status` filtered queries.

**Mitigation in Data Agent instructions:** The agent instructions explicitly route `stock_status` questions to `retail360_sm` rather than `retail360_om`.

---

### GQL two-hop simultaneous divergence

**Symptom:** NL2Ontology returns `Failed to translate NL query to ontology query` for questions requiring two hops from the same entity simultaneously.

**Example:**
```
"Products in stockout AND cheaper at competitors"
= InventoryStatus → Product ← CompetitorPrice   (two paths to same node)
```

**Root cause:** The NL2Ontology layer cannot resolve two diverging relationship paths from the same root entity in a single GQL MATCH clause.

**Workaround:** Pre-computed Gold tables (`gold_double_risk_products`, `gold_champion_stockout_alert`) materialized in the Silver-to-Gold notebook. These are the correct architectural pattern for this use case — not a workaround.

---

### DECIMAL type not supported in GQL

**Symptom:** GQL returns `DECIMAL datatypes are not supported yet` when aggregating numeric columns.

**Fix:** Cell 8 in `nb_silver_to_gold.py` casts all DECIMAL columns to DOUBLE across all Gold tables.

---

### Calculated DAX tables cannot reference Direct Lake

**Symptom:** Cannot create dimension tables using DAX `CALENDAR()` or `SUMMARIZE()` when the source table uses Direct Lake mode.

**Fix:** Create all dimension tables as physical Delta tables in Spark notebooks (Cell 10 in `nb_silver_to_gold.py`). Never create them as DAX calculated tables.

---

### Databricks Mirroring breaks after DROP TABLE

**Symptom:** After running `DROP TABLE + saveAsTable` in Databricks, the Fabric Mirrored Database shows "Couldn't access data" for that table.

**Root cause:** DROP TABLE resets Unity Catalog table registration and clears all grants. The Fabric Mirroring service principal loses SELECT access.

**Fix:** Always use `INSERT OVERWRITE` in Databricks notebooks. See `04_databricks/nb_customer_segmentation.py` for the correct pattern.

---

### GQL does not support GROUP BY

**Fix:** Use `RETURN DISTINCT column` instead of `GROUP BY column`.

---

## 18. Troubleshooting

### Agent returns "technical access issue"

1. In `retail360_lh` → **Manage OneLake security** → verify Data Agent identity has Read access
2. Republish the ontology to refresh the connection token
3. Check the Semantic Model is connected in the Data Agent Setup tab

### Mirroring shows stale data after PostgreSQL UPDATE

CDC propagation normally takes 2–5 minutes. If data has not propagated after 10 minutes:
1. In the Mirrored Database → **Replication status** → check for errors
2. Stop replication → wait 30 seconds → Start replication

### Gold table coverage_days shows very high values (~2000 days)

The `avg_daily_sales` join returned near-zero values. The fix in Cell 6 of `nb_silver_to_gold.py` uses `COUNT(DISTINCT order_date)` instead of a fixed 90-day window, giving accurate daily rates across the full data history.

### Power BI matrix showing %GT instead of actual values

In the matrix Values well → click dropdown on the measure → change from **% of grand total** to **Average** (for coverage days) or **Sum** (for stockout counts).

### Ontology property not found in GQL

Check the property name using:
```gql
MATCH (i:`InventoryStatus`)
RETURN i
LIMIT 1
```
Use exactly the names returned by this query — either the physical column name or the ontology rename depending on how the binding was configured.

### silver_weather weather_condition still returns French values

The Dataflow Gen2 `qry_weather.pq` reads raw JSON files which contain French `weather_label` values (`ensoleillé/nuageux/pluie/couvert`). These are intentionally kept in French because the Fabric IQ agent instructions include the translation mapping. If English is required, update `generate_data.py` to output English values and re-upload the weather files.
