-- ============================================================
-- Retail360 — PostgreSQL Post-Setup Verification
-- ============================================================

-- Row counts
SELECT 'stores'      AS tbl, COUNT(*) AS rows FROM stores
UNION ALL SELECT 'products',   COUNT(*) FROM products
UNION ALL SELECT 'orders',     COUNT(*) FROM orders
UNION ALL SELECT 'order_items',COUNT(*) FROM order_items
UNION ALL SELECT 'inventory',  COUNT(*) FROM inventory
ORDER BY rows DESC;

-- Date range
SELECT MIN(order_date) AS first, MAX(order_date) AS last FROM orders;

-- Store types (must be English)
SELECT store_type, COUNT(*) FROM stores GROUP BY store_type ORDER BY 2 DESC;

-- Channels (must be English)
SELECT channel, COUNT(*) FROM orders GROUP BY channel ORDER BY 2 DESC;

-- Inventory status (must be English)
SELECT stock_status, COUNT(*) AS cnt,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 1) AS pct
FROM inventory GROUP BY stock_status ORDER BY cnt DESC;

-- Verify logical replication (required for Fabric Mirroring)
SHOW wal_level;
SHOW azure.replication_support;
