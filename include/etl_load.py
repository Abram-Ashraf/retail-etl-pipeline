"""
etl_load.py
-----------
Load logic for the Retail Sales ETL pipeline.
Writes cleaned DataFrames into PostgreSQL using an upsert strategy
so the pipeline is idempotent (safe to re-run).
"""

import logging
import pandas as pd
from sqlalchemy import create_engine, text, MetaData

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# ENGINE FACTORY
# ─────────────────────────────────────────────────────────────
def get_engine(conn_str: str):
    """Return a SQLAlchemy engine for the given connection string."""
    return create_engine(conn_str, pool_pre_ping=True)


# ─────────────────────────────────────────────────────────────
# GENERIC UPSERT HELPER
# ─────────────────────────────────────────────────────────────
def upsert_dataframe(
    df: pd.DataFrame,
    table_name: str,
    pk_cols: list,
    engine,
    update_cols: list = None,
) -> int:
    """
    Insert rows from df into `table_name`.
    On conflict on `pk_cols`, update `update_cols` (or do nothing if None).
    Returns number of rows processed.
    """
    if df.empty:
        logger.info(f"[{table_name}] Nothing to load – DataFrame is empty.")
        return 0

    records = df.to_dict(orient="records")

    # Reflect the actual SQLAlchemy Table object from the live database
    from sqlalchemy.dialects.postgresql import insert as pg_insert
    metadata = MetaData()
    metadata.reflect(bind=engine, only=[table_name])
    table = metadata.tables[table_name]

    with engine.begin() as conn:
        stmt = pg_insert(table).values(records)

        if update_cols:
            update_dict = {col: stmt.excluded[col] for col in update_cols}
            stmt = stmt.on_conflict_do_update(
                index_elements=pk_cols,
                set_=update_dict,
            )
        else:
            stmt = stmt.on_conflict_do_nothing(index_elements=pk_cols)

        conn.execute(stmt)

    logger.info(f"[{table_name}] Upserted {len(records):,} rows.")
    return len(records)


# ─────────────────────────────────────────────────────────────
# TABLE-SPECIFIC LOADERS
# ─────────────────────────────────────────────────────────────
def load_dim_customer(df: pd.DataFrame, engine) -> None:
    upsert_dataframe(
        df, "dim_customer",
        pk_cols=["customer_id"],
        engine=engine,
        update_cols=["customer_name", "city", "country"],
    )


def load_dim_product(df: pd.DataFrame, engine) -> None:
    upsert_dataframe(
        df, "dim_product",
        pk_cols=["product_id"],
        engine=engine,
        update_cols=["product_name", "category", "unit_cost"],
    )


def load_dim_date(df: pd.DataFrame, engine) -> None:
    # Convert date objects to strings for SQLAlchemy compatibility
    df = df.copy()
    df["date_id"] = df["date_id"].astype(str)
    upsert_dataframe(
        df, "dim_date",
        pk_cols=["date_id"],
        engine=engine,
        update_cols=None,  # date attributes never change
    )


def load_fact_sales(df: pd.DataFrame, engine) -> None:
    df = df.copy()
    df["order_date"] = df["order_date"].astype(str)
    upsert_dataframe(
        df, "fact_sales",
        pk_cols=["order_id", "product_id"],
        engine=engine,
        update_cols=["order_date", "customer_id", "quantity", "unit_price", "unit_cost"],
    )


# ─────────────────────────────────────────────────────────────
# SUMMARY QUERY (for logging/audit after load)
# ─────────────────────────────────────────────────────────────
def print_load_summary(engine) -> None:
    summary_sql = """
        SELECT
            'dim_customer'  AS table_name, COUNT(*) AS row_count FROM dim_customer
        UNION ALL
        SELECT 'dim_product',  COUNT(*) FROM dim_product
        UNION ALL
        SELECT 'dim_date',     COUNT(*) FROM dim_date
        UNION ALL
        SELECT 'fact_sales',   COUNT(*) FROM fact_sales
        ORDER BY table_name;
    """
    with engine.connect() as conn:
        result = conn.execute(text(summary_sql))
        rows = result.fetchall()

    logger.info("─── DWH Load Summary ───────────────────────")
    for table_name, row_count in rows:
        logger.info(f"  {table_name:<20} {row_count:>6,} rows")
    logger.info("────────────────────────────────────────────")