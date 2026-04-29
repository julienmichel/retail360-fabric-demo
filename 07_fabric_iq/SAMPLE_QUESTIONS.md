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
