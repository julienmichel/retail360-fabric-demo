-- ============================================================
-- Retail360 — Fabric SQL Database (retail360_sql)
-- Run in SQL Database query editor
-- ============================================================

CREATE TABLE dbo.transfer_orders (
    transfer_id   INT          IDENTITY(1,1) PRIMARY KEY,
    from_store_id INT          NOT NULL,
    to_store_id   INT          NOT NULL,
    product_id    INT          NOT NULL,
    quantity      INT          NOT NULL,
    reason        VARCHAR(100),
    created_at    DATETIME2    DEFAULT GETDATE(),
    status        VARCHAR(20)  DEFAULT 'pending'
);

CREATE TABLE dbo.customer_offers (
    offer_id     INT          IDENTITY(1,1) PRIMARY KEY,
    customer_id  INT          NOT NULL,
    offer_type   VARCHAR(50),
    discount_pct DECIMAL(5,2),
    category     VARCHAR(60),
    valid_until  DATE,
    generated_at DATETIME2    DEFAULT GETDATE()
);

CREATE TABLE dbo.active_alerts (
    alert_id    INT          IDENTITY(1,1) PRIMARY KEY,
    store_id    INT,
    product_id  INT,
    alert_type  VARCHAR(50),
    severity    VARCHAR(20),
    message     VARCHAR(500),
    created_at  DATETIME2    DEFAULT GETDATE(),
    resolved_at DATETIME2,
    status      VARCHAR(20)  DEFAULT 'active'
);

CREATE TABLE dbo.incidents (
    incident_id   INT          IDENTITY(1,1) PRIMARY KEY,
    store_id      INT,
    incident_type VARCHAR(50),
    description   VARCHAR(500),
    created_at    DATETIME2    DEFAULT GETDATE(),
    resolved_at   DATETIME2,
    status        VARCHAR(20)  DEFAULT 'open'
);

-- Seed data for demo walkthrough

INSERT INTO dbo.transfer_orders (from_store_id,to_store_id,product_id,quantity,reason,status) VALUES
(12,7,342,50,'stockout_reflex','pending'),
(34,12,108,30,'stockout_reflex','pending'),
(89,45,215,20,'critical_stock_reflex','pending'),
(23,67,399,15,'stockout_reflex','completed'),
(56,89,77,40,'critical_stock_reflex','completed'),
(11,33,189,25,'stockout_reflex','pending'),
(78,12,456,10,'critical_stock_reflex','in_progress'),
(45,23,301,35,'stockout_reflex','pending'),
(90,56,88,60,'stockout_reflex','completed'),
(15,78,412,45,'critical_stock_reflex','in_progress');

INSERT INTO dbo.customer_offers (customer_id,offer_type,discount_pct,category,valid_until) VALUES
(102345,'loyalty_discount',15.00,'Électronique','2026-05-01'),
(234567,'win_back',20.00,'Mode & Textile','2026-04-30'),
(345678,'loyalty_discount',10.00,'Alimentation','2026-05-15'),
(456789,'win_back',25.00,'Sports & Loisirs','2026-04-25'),
(567890,'loyalty_discount',15.00,'Beauté & Santé','2026-05-10'),
(678901,'win_back',20.00,'Maison & Décoration','2026-04-28'),
(789012,'loyalty_discount',10.00,'Jouets & Enfants','2026-05-20'),
(890123,'win_back',30.00,'Électronique','2026-04-22'),
(901234,'loyalty_discount',15.00,'Alimentation','2026-05-05'),
(112345,'win_back',20.00,'Mode & Textile','2026-04-29');

INSERT INTO dbo.active_alerts (store_id,product_id,alert_type,severity,message,status) VALUES
(7,342,'stockout','critical','Product 342 out of stock at store 7','active'),
(12,108,'stockout','critical','Product 108 out of stock at store 12','active'),
(45,215,'critical_stock','high','Product 215 below safety stock at store 45','active'),
(23,399,'stockout','critical','Product 399 out of stock at store 23','resolved'),
(56,77,'critical_stock','high','Product 77 below safety stock at store 56','active'),
(33,189,'stockout','critical','Product 189 out of stock at store 33','active'),
(12,456,'critical_stock','high','Product 456 below safety stock at store 12','active'),
(89,301,'sales_spike','medium','Unusual sales spike at store 89','active'),
(15,412,'stockout','critical','Product 412 out of stock at store 15','active'),
(67,88,'pos_silence','high','No POS transactions for 45 minutes','resolved');

-- Verify
SELECT 'transfer_orders' AS tbl, COUNT(*) AS rows FROM dbo.transfer_orders
UNION ALL SELECT 'customer_offers', COUNT(*) FROM dbo.customer_offers
UNION ALL SELECT 'active_alerts',   COUNT(*) FROM dbo.active_alerts
UNION ALL SELECT 'incidents',       COUNT(*) FROM dbo.incidents;
