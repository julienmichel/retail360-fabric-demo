# Retail360 — Microsoft Fabric Demo

A comprehensive retail analytics demo showcasing the full Microsoft Fabric stack.
Built and validated during a full end-to-end build session — all fixes and known
limitations are documented.

## What this demo shows

| Capability | Technology | Status |
|---|---|---|
| Multi-source unification | PostgreSQL + Databricks + Files → OneLake | ✓ |
| Zero-ETL ingestion | Fabric Mirroring (PostgreSQL CDC + Databricks) | ✓ |
| ML-powered insights | Azure Databricks — Prophet + K-Means | ✓ |
| Medallion architecture | Bronze → Silver → Gold Delta tables | ✓ |
| Semantic layer | Fabric IQ Ontology — entity graph + business rules | ✓ |
| Natural language analytics | Fabric Data Agent — 3-layer routing | ✓ |
| High-performance BI | Power BI Direct Lake + Copilot | ✓ |
| Operational automation | Fabric SQL Database + agent-created records | ✓ |

## Architecture

```
Azure PostgreSQL ──► Fabric Mirroring (CDC) ──► silver_pg_*
External Files   ──► Dataflow Gen2          ──► silver_weather
                                                silver_promotions
                                                silver_competitor_prices
Azure Databricks ──► Unity Catalog          ──► silver_adx_*
  Prophet (forecast)   └► Mirroring
  K-Means (RFM)
                                    │
                              Spark Notebooks
                                    │
                    ┌───────────────┼──────────────────┐
                    │               │                  │
             gold_sales_daily  gold_inventory_status  gold_forecast_vs_actual
             gold_double_risk_products               (pre-computed cross-entity)
             gold_champion_stockout_alert            (pre-computed cross-entity)
             dim_calendar / dim_category / dim_region / dim_channel
                    │
          ┌─────────┼──────────────────┐
          │         │                  │
    retail360_sm  retail360_om   retail360_sql
  (Semantic Model) (Ontology)    (operational)
    Direct Lake   Entity graph   transfer_orders
    DAX measures  Relationships  customer_offers
          │         │            active_alerts
    Power BI    Data Agent
    Copilot
```

## Quick start

```bash
git clone https://github.com/your-org/retail360-fabric-demo
cd retail360-fabric-demo
pip install -r config/requirements.txt
cp config/config.example.json config/config.json
# Edit config.json with your credentials
python 01_postgresql/generate_data.py
```

Then follow [SETUP_GUIDE.md](SETUP_GUIDE.md) step by step.

## Repository structure

```
retail360-fabric-demo/
├── README.md
├── SETUP_GUIDE.md                      ← complete step-by-step guide
├── 01_postgresql/
│   ├── generate_data.py                ← generates all source data
│   ├── fix_labels.sql                  ← English label fixes (PostgreSQL)
│   └── verify_data.sql                 ← post-setup verification
├── 02_fabric_workspace/
│   └── create_workspace.py             ← Fabric REST API setup
├── 03_lakehouse/
│   └── nb_silver_to_gold.py            ← all Gold tables + all fixes
├── 04_databricks/
│   ├── nb_demand_forecast.py           ← Prophet 90-day forecast
│   └── nb_customer_segmentation.py     ← K-Means RFM segmentation
├── 05_dataflows/
│   ├── qry_weather.pq                  ← Power Query M (weather JSON)
│   ├── qry_promotions.pq               ← Power Query M (promotions CSV)
│   └── qry_competitor.pq               ← Power Query M (competitor CSV)
├── 06_semantic_model/
│   ├── measures.dax                    ← 30+ DAX measures
│   └── relationships.json              ← 9 relationship definitions
├── 07_fabric_iq/
│   └── agent_instructions.txt          ← complete Data Agent instructions
├── 08_sql_database/
│   └── create_operational_tables.sql   ← DDL + demo seed data
└── config/
    ├── config.example.json             ← template
    └── requirements.txt                ← Python dependencies
```

## 6 demo questions (validated)

| # | Question | Source layer | Validated |
|---|---|---|---|
| 1 | Which stores in Île-de-France have products in critical or stockout status? | Ontology → Semantic Model | ✓ |
| 2 | Compare gross margin by category 2024 vs 2023 | Semantic Model | ✓ |
| 3 | Which categories have worst forecast accuracy AND highest price pressure? | Semantic Model (2-step) | ✓ |
| 4 | Which products are both out of stock AND cheaper at competitors? | Pre-computed Gold | ✓ |
| 5 | Which Champion customers bought products now in stockout? | Pre-computed Gold | ✓ |
| 6 | How many transfer orders did our agent create this week? | SQL Database | ✓ |

## Data volumes

| Table | Rows | Source |
|---|---|---|
| silver_pg_orders | ~825,000 | PostgreSQL Mirroring |
| silver_pg_order_items | ~2,064,000 | PostgreSQL Mirroring |
| gold_sales_daily | ~1,500,000+ | Spark aggregation |
| gold_inventory_status | ~52,000 | Spark generation |
| silver_adx_customer_segments | ~405,000 | Databricks K-Means |
| silver_competitor_prices | ~36,750 | Dataflow Gen2 |
| silver_weather | ~78,000 | Dataflow Gen2 |

## Known limitations (Fabric IQ preview)

### 1. GQL graph index caching
After rebuilding a Gold table, the GQL query engine may continue
returning stale data for hours because the graph index is not
immediately invalidated. The binding preview reads Delta directly
(correct) but GQL reads the cached index.

**Workaround:** Route `stock_status` filter questions to the
Semantic Model. The Data Agent instructions are pre-configured
to do this automatically.

### 2. Two-hop simultaneous divergence
NL2Ontology cannot resolve two diverging relationship paths from
the same entity in one query (e.g. Product → InventoryStatus AND
Product → CompetitorPrice simultaneously).

**Workaround:** Pre-computed Gold tables
(`gold_double_risk_products`, `gold_champion_stockout_alert`).

### 3. DECIMAL not supported in GQL
The GQL engine rejects DECIMAL column aggregations.

**Fix:** `nb_silver_to_gold.py` Cell 8 casts all DECIMAL to DOUBLE.

### 4. DROP TABLE breaks Databricks Mirroring
Dropping and recreating tables in Databricks resets Unity Catalog
grants and breaks the Fabric Mirroring connection.

**Fix:** Always use `INSERT OVERWRITE` in Databricks notebooks.

### 5. Calculated DAX tables incompatible with Direct Lake
Cannot create dimension tables using DAX when source uses Direct Lake.

**Fix:** Create all dimension tables as physical Delta tables in Spark.

## License

MIT — free to use for demos, POCs, and internal presentations.
