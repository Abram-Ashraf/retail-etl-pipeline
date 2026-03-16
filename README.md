# 🛒 Retail Sales ETL Pipeline

An end-to-end data engineering project that extracts retail sales data from CSV files, transforms and cleans it using Python and pandas, loads it into a PostgreSQL data warehouse, and automates the entire workflow using Apache Airflow — all containerised with Docker.

---

## 📋 Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Data Model](#data-model)
- [Getting Started](#getting-started)
- [Running the Pipeline](#running-the-pipeline)
- [Querying the Warehouse](#querying-the-warehouse)
- [DAG Overview](#dag-overview)
- [ETL Logic](#etl-logic)

---

## Project Overview

This project implements a production-ready ETL pipeline for retail sales data:

- **Extract** — reads raw CSV files from a shared `data/` directory
- **Transform** — validates, cleans, and reshapes data using pandas
- **Load** — upserts structured data into a PostgreSQL star-schema data warehouse
- **Orchestrate** — automates the pipeline on a daily schedule using Apache Airflow

The pipeline is fully idempotent — re-running it never creates duplicate records.

---

## Architecture

```
CSV Files (data/)
      │
      ▼
┌─────────────┐     ┌──────────────────┐     ┌─────────────────────┐
│   Extract   │────▶│    Transform     │────▶│        Load         │
│  (pandas)   │     │  (clean/validate)│     │  (PostgreSQL DWH)   │
└─────────────┘     └──────────────────┘     └─────────────────────┘
                                                        │
                              ┌─────────────────────────┼──────────────────────┐
                              ▼                         ▼                      ▼
                       dim_customer             dim_product              dim_date
                                                        │
                                                        ▼
                                                  fact_sales
```

**Airflow DAG task flow:**
```
extract → transform → load_dimensions → load_facts → summary
```

---

## Tech Stack

| Component | Technology |
|---|---|
| Orchestration | Apache Airflow 2.x (CeleryExecutor) |
| Database | PostgreSQL 13 |
| Transform | Python 3.10, pandas 2.x |
| ORM / DB Driver | SQLAlchemy + psycopg2 |
| Message Broker | Redis 7 |
| Containerisation | Docker + Docker Compose |

---

## Project Structure

```
retail-etl-pipeline/
├── dags/
│   └── retail_sales_etl_dag.py     # Airflow DAG definition
├── include/
│   ├── etl_transform.py            # Extract + Transform logic
│   └── etl_load.py                 # Load logic (upsert to PostgreSQL)
├── data/
│   └── sales_2025_01.csv           # Source CSV files (drop new files here)
├── postgres/
│   └── init/
│       └── 01_create_retail_dwh.sql  # Auto-runs on first container start
├── logs/                           # Airflow task logs (auto-generated)
├── config/                         # Airflow config overrides
├── plugins/                        # Custom Airflow plugins
├── docker-compose.yaml
└── README.md
```

---

## Data Model

Star schema in the `retail_dwh` PostgreSQL database:

```
          dim_customer          dim_product
         ┌────────────┐        ┌────────────┐
         │customer_id │        │product_id  │
         │customer_name        │product_name│
         │city        │        │category    │
         │country     │        │unit_cost   │
         └─────┬──────┘        └──────┬─────┘
               │                      │
               └──────────┬───────────┘
                          │
                   ┌──────▼───────┐
                   │  fact_sales  │
                   │─────────────│
                   │sale_id (PK) │
                   │order_id     │
                   │order_date ──┼──── dim_date
                   │customer_id  │    ┌──────────┐
                   │product_id   │    │date_id   │
                   │quantity     │    │day/month │
                   │unit_price   │    │quarter   │
                   │unit_cost    │    │year      │
                   │total_revenue│    │is_weekend│
                   │gross_profit │    └──────────┘
                   │profit_margin│
                   └─────────────┘
```

### Computed columns in `fact_sales`

| Column | Formula |
|---|---|
| `total_revenue` | `quantity × unit_price` |
| `total_cost` | `quantity × unit_cost` |
| `gross_profit` | `quantity × (unit_price − unit_cost)` |
| `profit_margin` | `(unit_price − unit_cost) / unit_price` |

---

## Getting Started

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running
- At least **4 GB RAM** allocated to Docker
- Ports **8080** and **5433** free on your machine

### 1. Clone the repository

```bash
git clone https://github.com/your-username/retail-etl-pipeline.git
cd retail-etl-pipeline
```

### 2. Create the `.env` file

**Linux / macOS:**
```bash
echo "AIRFLOW_UID=$(id -u)" > .env
```

**Windows (PowerShell):**
```powershell
"AIRFLOW_UID=50000" | Out-File -FilePath .env -Encoding utf8NoBOM
```

### 3. Initialise Airflow

```bash
docker compose up airflow-init
```

Wait until you see:
```
airflow-init exited with code 0
```

### 4. Start the full stack

```bash
docker compose up -d
```

Wait ~60 seconds, then verify all services are healthy:
```bash
docker compose ps
```

All services should show `healthy`.

---

## Running the Pipeline

### Trigger via Airflow UI

1. Open **http://localhost:8080**
2. Login: `airflow` / `airflow`
3. Find the `retail_sales_etl` DAG and toggle it **ON**
4. Click **▶ Trigger DAG**
5. Watch all 5 tasks turn green

### Trigger via CLI

```bash
docker compose exec airflow-webserver airflow dags trigger retail_sales_etl
```

### Adding new CSV files

Drop any new `.csv` file (with the same column schema) into the `data/` folder. The next DAG run will automatically pick it up. Existing records are updated via upsert — no duplicates are created.

### Expected CSV schema

```
order_id, order_date, customer_id, customer_name, city, country,
product_id, product_name, category, quantity, unit_price, unit_cost
```

---

## Querying the Warehouse

Connect to the warehouse using any PostgreSQL client:

| Field | Value |
|---|---|
| Host | `localhost` |
| Port | `5433` |
| Database | `retail_dwh` |
| Username | `dwh_user` |
| Password | `dwh_pass` |

### Sample queries

**Revenue and profit by category:**
```sql
SELECT
    p.category,
    COUNT(*)                          AS num_orders,
    SUM(f.quantity)                   AS units_sold,
    SUM(f.total_revenue)              AS total_revenue,
    SUM(f.gross_profit)               AS total_profit,
    ROUND(AVG(f.profit_margin)*100,1) AS avg_margin_pct
FROM fact_sales f
JOIN dim_product p USING (product_id)
GROUP BY p.category
ORDER BY total_revenue DESC;
```

**Top customers by gross profit:**
```sql
SELECT
    c.customer_name,
    c.city,
    COUNT(*)             AS orders,
    SUM(f.total_revenue) AS revenue,
    SUM(f.gross_profit)  AS profit
FROM fact_sales f
JOIN dim_customer c USING (customer_id)
GROUP BY c.customer_name, c.city
ORDER BY profit DESC
LIMIT 10;
```

**Monthly sales trend:**
```sql
SELECT
    d.year,
    d.month_name,
    SUM(f.total_revenue) AS revenue,
    SUM(f.gross_profit)  AS profit
FROM fact_sales f
JOIN dim_date d ON f.order_date = d.date_id
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;
```

---

## DAG Overview

**DAG ID:** `retail_sales_etl`  
**Schedule:** Daily at 06:00 UTC  
**Max active runs:** 1  
**Retries:** 2 (5-minute delay)

| Task | Description |
|---|---|
| `extract` | Reads all `*.csv` files from `data/`, pushes raw DataFrame to XCom |
| `transform` | Cleans data, enforces business rules, builds 4 DataFrames |
| `load_dimensions` | Upserts `dim_customer`, `dim_product`, `dim_date` |
| `load_facts` | Upserts `fact_sales` after dimensions are committed |
| `summary` | Logs row counts for all DWH tables as an audit report |

---

## ETL Logic

### Data Quality Rules (applied in `etl_transform.py`)

| Rule | How enforced |
|---|---|
| No duplicate rows | `pandas.drop_duplicates()` |
| Valid `order_date` | `pd.to_datetime` with coerce + dropna |
| Numeric quantity / price / cost | `pd.to_numeric` with coerce + dropna |
| `quantity > 0` | pandas filter |
| `unit_price > 0` | pandas filter |
| `unit_cost >= 0` | pandas filter |
| No duplicate order + product | PostgreSQL `UNIQUE(order_id, product_id)` |
| Referential integrity | PostgreSQL `FOREIGN KEY` constraints |

### Upsert strategy (`etl_load.py`)

All loads use PostgreSQL `ON CONFLICT DO UPDATE`, making every pipeline run fully idempotent. Re-running the DAG updates existing records rather than duplicating them.
