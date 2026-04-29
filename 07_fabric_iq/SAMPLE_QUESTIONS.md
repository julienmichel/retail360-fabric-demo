# Retail360 — Sample Questions

Reference guide for the Data Agent and Power BI Q&A.
Questions are organized by intelligence layer and complexity.

---

## Data Agent — 6 core demo questions

Run these in order during the demo — they tell a complete story in ~10 minutes.

| # | Question | Source | Layer |
|---|---|---|---|
| Q1 | Which stores in Île-de-France have products in critical or stockout status? | retail360_om | Ontology single hop |
| Q2 | Compare our gross margin by product category for 2024 vs 2023 and identify which categories are declining | retail360_sm | Semantic Model time intelligence |
| Q3 | Which product categories have the worst forecast accuracy AND the highest competitor price pressure? | retail360_sm | Semantic Model multi-step |
| Q4 | Which products are both out of stock AND being sold cheaper by competitors right now? | gold_double_risk_products | Pre-computed Gold |
| Q5 | Which Champion customers recently bought products that are now in stockout? Prioritize by lifetime value. | gold_champion_stockout_alert | Pre-computed Gold |
| Q6 | How many transfer orders did our replenishment agent create this week and what is their current status? | retail360_sql | SQL Database |

---

## Data Agent — by layer

### Ontology — entity lookup

```
"Which stores are hypermarkets?"
"Which stores in Lyon are convenience stores?"
"How many stores do we have in Bretagne?"
"Which promotions are currently active?"
"Show me all active products in the Électronique category"
"What is the average selling price of products in Sports & Loisirs?"
"Which customers are in the At Risk segment?"
"How many Champion customers do we have?"
```

### Ontology — single hop relationship traversal

```
"Which products are cheaper at BonPrix right now?"
"Which products does TendancePlus undercut on price?"
"Which products have a forecast accuracy below 75%?"
"Which products have the highest forecasted demand for the next 30 days?"
"Which stores in Paris experience rainy weather?"
"Show me products where NeoShop has a price advantage above 10%"
"Which products are forecasted to sell more than 50 units per day?"
```

### Semantic Model — KPIs

```
"What is our overall forecast accuracy?"
"How many products are currently in stockout?"
"What is our gross margin percentage overall?"
"What is our average stock coverage in days?"
"How many Champion customers do we have?"
"What percentage of products are cheaper at competitors?"
"What is our average basket size?"
"What is our total revenue year to date?"
```

### Semantic Model — time intelligence

```
"Compare our total revenue for 2024 vs 2023"
"What was our revenue last November vs November the year before?"
"Which month had the highest revenue in 2024?"
"What is our revenue this month compared to last month?"
"How did Black Friday 2024 compare to Black Friday 2023?"
"Which quarter had the best gross margin in 2023?"
"What is our year to date revenue compared to last year at the same period?"
"How has our stockout rate evolved over the last 6 months?"
```

### Semantic Model — rankings and filters

```
"Which are the top 5 stores by revenue in 2024?"
"Which 3 regions have the highest stockout rate?"
"Which product categories have the highest gross margin?"
"Which stores have less than 7 days of average stock coverage?"
"Which competitor is cheapest on average across all categories?"
"Which customer segments have the highest average lifetime value?"
"Which 10 products have the worst forecast accuracy?"
"Which promotions generated the highest revenue uplift?"
```

### Pre-computed Gold — cross-entity

```
"Which products are both out of stock AND cheaper at competitors?"
"Show me products in critical stock that are also being undercut by BonPrix"
"Which Champion customers recently bought products now in stockout?"
"How many Champion customers are affected by current stockouts?"
"Which high-value customers should we contact first about stockout products they buy?"
```

### SQL Database — operational

```
"How many transfer orders are pending right now?"
"What is the breakdown of transfer orders by status this week?"
"Show me all active critical alerts"
"How many retention offers were sent to customers this week?"
"What is the split between win-back and loyalty discount offers?"
"Show me transfer orders created in the last 24 hours"
"Which stores have the most active alerts?"
"How many incidents are currently open?"
```

### Multi-step synthesis

```
"Which product categories have the worst forecast accuracy
 AND the highest competitor price pressure?"

"For Champion customers who recently bought stockout products,
 what is their total lifetime value at risk?"

"Which regions have both the highest stockout rate
 AND the most pending transfer orders?"

"Compare revenue uplift across all promotions
 and identify which ones delivered below the expected traffic boost"
```

---

## Power BI Q&A — suggested questions

Type these directly into the Power BI Q&A visual or Copilot panel.

### Executive Overview page

```
total revenue by region as map
top 10 stores by revenue this year
revenue by month 2024 vs 2023 as line chart
gross margin by category as bar chart
customer segment distribution as donut
revenue by channel this year
```

### Inventory Intelligence page

```
stockout rate by region as bar chart
products in stockout by category as matrix
average coverage days by store format
stores with coverage days less than 7
competitor price gap by category as bar chart
products cheaper at BonPrix as table
```

### Forecast & ML page

```
forecast accuracy by category as bar chart
forecast vs actual last 6 months as line chart
products with forecast accuracy below 75 percent
30 day demand forecast by category
weather condition impact on revenue as scatter
mae forecast by category
```

### Intelligence 360° page

```
revenue uplift by promotion as bar chart
customer segments by lifetime value as bubble chart
stockout rate by store format and category as heatmap
average price gap by competitor as bar chart
at risk customers count by region
champion customers by region as map
```

---

## Power BI Copilot — narrative prompts

Use these in the Copilot side panel for AI-generated summaries.

```
"Summarize the inventory situation this month"
"What are the key trends in revenue over the last quarter?"
"Explain the forecast accuracy results by category"
"Which stores need urgent attention based on current data?"
"Summarize the competitive pricing situation"
"What is the revenue impact of our promotional events?"
"Identify the top risks in our current inventory"
"Which customer segments should we prioritize for retention actions?"
```

---

## Routing reference

```
Question type                    → Source
─────────────────────────────────────────────────────────────
Entity attribute lookup          → retail360_om (Ontology)
Single relationship hop          → retail360_om (Ontology)
Stock status filter              → retail360_sm (always — ontology index may be stale)
KPI or measure                   → retail360_sm (Semantic Model)
Time intelligence (YoY, MTD)     → retail360_sm (Semantic Model)
Rankings / top N                 → retail360_sm (Semantic Model)
Stockout + competitor cross      → gold_double_risk_products
Champion + stockout cross        → gold_champion_stockout_alert
Pending orders / alerts / offers → retail360_sql (SQL Database)
```

---

## Business rules (for interpreting results)

```
Coverage_Days < 7        = urgent replenishment needed
Coverage_Days > 30       = potential overstock
Forecast_Accuracy < 0.75 = poor — model retraining recommended
Forecast_Accuracy > 0.90 = high confidence — safe for procurement
Price_Gap_Pct negative   = competitor undercuts our price
Stockout Rate > 5%       = critical inventory situation
At Risk recency > 90     = high priority retention action
Inactive + LTV > 500     = high value win-back opportunity
Gross Margin < 15%       = underperforming store or category
Gross Margin 15–25%      = on track
Gross Margin > 25%       = outperforming
```

---

## GQL queries for Fabric Graph explorer

Use these directly in the Fabric IQ Graph → GQL editor.
**All entity type names and property names require backticks.**

### Products with competitor price disadvantage

```gql
MATCH (c:`CompetitorPrice`)-[:`benchmarked_against`]->(p:`Product`)
WHERE c.`Competitor_Is_Cheaper` = true
RETURN p.`name`              AS product_name,
       p.`Product_Category`  AS category,
       c.`Competitor_Name`   AS competitor,
       c.`Price_Gap_Pct`     AS price_gap_pct,
       c.`our_price`         AS our_price,
       c.`competitor_price`  AS competitor_price
ORDER BY c.`Price_Gap_Pct` ASC
LIMIT 50
```

### Products with poor forecast accuracy

```gql
MATCH (f:`ForecastVsActual`)-[:`is_forecasted_for`]->(p:`Product`)
WHERE f.`Forecast_Accuracy` < 0.75
RETURN p.`name`              AS product_name,
       p.`Product_Category`  AS category,
       f.`Forecast_Accuracy` AS forecast_accuracy,
       f.`Forecasted_Quantity` AS forecasted_qty
ORDER BY f.`Forecast_Accuracy` ASC
LIMIT 50
```

### High demand forecast but low stock

```gql
MATCH (i:`InventoryStatus`)-[:`tracked_in_inventory`]->(p:`Product`),
      (d:`DemandForecast`)-[:`in_demand_forecast`]->(p)
WHERE i.`Stock_On_Hand` < 10
  AND d.`Forecasted_Quantity` > 10
RETURN p.`name`                AS product_name,
       p.`Product_Category`    AS category,
       i.`store_name`          AS store_name,
       i.`region`              AS region,
       i.`Stock_On_Hand`       AS stock_on_hand,
       d.`Forecasted_Quantity` AS forecasted_qty
ORDER BY i.`Stock_On_Hand` ASC
LIMIT 50
```

### High confidence forecast + price disadvantage

```gql
MATCH (f:`ForecastVsActual`)-[:`is_forecasted_for`]->(p:`Product`),
      (c:`CompetitorPrice`)-[:`benchmarked_against`]->(p)
WHERE f.`Forecast_Accuracy` > 0.90
  AND c.`Competitor_Is_Cheaper` = true
RETURN p.`name`               AS product_name,
       p.`Product_Category`   AS category,
       f.`Forecast_Accuracy`  AS forecast_accuracy,
       c.`Competitor_Name`    AS competitor,
       c.`Price_Gap_Pct`      AS price_gap_pct
ORDER BY c.`Price_Gap_Pct` ASC
LIMIT 50
```

### Known GQL limitations

```
✗  GROUP BY not supported → use RETURN DISTINCT instead
✗  Stock_Health_Status filter may return stale values
     (graph index cache — use Semantic Model for stock status)
✗  Two simultaneous diverging hops from same entity fail
     NL2Ontology translation → use pre-computed Gold tables
✗  DECIMAL aggregation not supported → all Gold tables cast to DOUBLE
```
