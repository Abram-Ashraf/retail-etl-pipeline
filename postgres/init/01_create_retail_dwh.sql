-- ============================================================
-- Retail Data Warehouse Initialization Script
-- ============================================================

CREATE DATABASE retail_dwh;
\c retail_dwh;

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'dwh_user') THEN
        CREATE ROLE dwh_user WITH LOGIN PASSWORD 'dwh_pass';
    END IF;
END
$$;

GRANT ALL PRIVILEGES ON DATABASE retail_dwh TO dwh_user;

-- Dimension: Customers
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id     VARCHAR(20) PRIMARY KEY,
    customer_name   VARCHAR(100) NOT NULL,
    city            VARCHAR(100),
    country         VARCHAR(100),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension: Products
CREATE TABLE IF NOT EXISTS dim_product (
    product_id      VARCHAR(20) PRIMARY KEY,
    product_name    VARCHAR(150) NOT NULL,
    category        VARCHAR(100),
    unit_cost       NUMERIC(12, 2),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension: Date
CREATE TABLE IF NOT EXISTS dim_date (
    date_id         DATE PRIMARY KEY,
    day             INTEGER,
    month           INTEGER,
    month_name      VARCHAR(20),
    quarter         INTEGER,
    year            INTEGER,
    day_of_week     INTEGER,
    day_name        VARCHAR(20),
    is_weekend      BOOLEAN
);

-- Fact: Sales
CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id         SERIAL PRIMARY KEY,
    order_id        INTEGER NOT NULL,
    order_date      DATE NOT NULL,
    customer_id     VARCHAR(20) REFERENCES dim_customer(customer_id),
    product_id      VARCHAR(20) REFERENCES dim_product(product_id),
    quantity        INTEGER NOT NULL,
    unit_price      NUMERIC(12, 2) NOT NULL,
    unit_cost       NUMERIC(12, 2) NOT NULL,
    total_revenue   NUMERIC(14, 2) GENERATED ALWAYS AS (quantity * unit_price) STORED,
    total_cost      NUMERIC(14, 2) GENERATED ALWAYS AS (quantity * unit_cost) STORED,
    gross_profit    NUMERIC(14, 2) GENERATED ALWAYS AS (quantity * (unit_price - unit_cost)) STORED,
    profit_margin   NUMERIC(8, 4) GENERATED ALWAYS AS (
                        CASE WHEN unit_price > 0
                             THEN ROUND((unit_price - unit_cost) / unit_price, 4)
                             ELSE 0 END
                    ) STORED,
    loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(order_id, product_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_sales_date     ON fact_sales(order_date);
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer ON fact_sales(customer_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product  ON fact_sales(product_id);

GRANT ALL PRIVILEGES ON ALL TABLES    IN SCHEMA public TO dwh_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO dwh_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES    TO dwh_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO dwh_user;
