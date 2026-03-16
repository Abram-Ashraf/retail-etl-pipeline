"""
etl_transform.py
----------------
Extract and Transform logic for the Retail Sales ETL pipeline.
Reads CSV files from the data/ folder, cleans/validates them,
and returns structured DataFrames ready for loading into the DWH.
"""

import os
import logging
import pandas as pd
from pathlib import Path
from typing import Tuple

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# REQUIRED COLUMNS & DTYPES
# ─────────────────────────────────────────────────────────────
REQUIRED_COLUMNS = [
    "order_id", "order_date", "customer_id", "customer_name",
    "city", "country", "product_id", "product_name",
    "category", "quantity", "unit_price", "unit_cost",
]

NUMERIC_COLS = ["quantity", "unit_price", "unit_cost"]


# ─────────────────────────────────────────────────────────────
# EXTRACT
# ─────────────────────────────────────────────────────────────
def extract_csv_files(data_dir: str) -> pd.DataFrame:
    """
    Read all CSV files in `data_dir` and concatenate them
    into a single raw DataFrame.
    """
    data_path = Path(data_dir)
    csv_files = sorted(data_path.glob("*.csv"))

    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {data_dir}")

    frames = []
    for f in csv_files:
        logger.info(f"Reading file: {f.name}")
        df = pd.read_csv(f, dtype=str)  # read everything as str first
        df["_source_file"] = f.name
        frames.append(df)

    raw = pd.concat(frames, ignore_index=True)
    logger.info(f"Extracted {len(raw):,} rows from {len(csv_files)} file(s).")
    return raw


# ─────────────────────────────────────────────────────────────
# VALIDATE
# ─────────────────────────────────────────────────────────────
def validate_columns(df: pd.DataFrame) -> None:
    """Raise ValueError if any required column is missing."""
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


# ─────────────────────────────────────────────────────────────
# TRANSFORM
# ─────────────────────────────────────────────────────────────
def transform(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Clean and transform raw DataFrame.

    Returns:
        dim_customer  – deduplicated customer dimension
        dim_product   – deduplicated product dimension
        dim_date      – calendar date dimension for all order dates
        fact_sales    – cleaned fact records
    """
    validate_columns(df)

    # ── 1. Strip whitespace from all string columns ──────────
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(lambda s: s.str.strip())

    # ── 2. Drop fully duplicate rows ─────────────────────────
    before = len(df)
    df.drop_duplicates(inplace=True)
    logger.info(f"Dropped {before - len(df)} duplicate rows.")

    # ── 3. Cast dates ─────────────────────────────────────────
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    invalid_dates = df["order_date"].isna().sum()
    if invalid_dates:
        logger.warning(f"Dropping {invalid_dates} rows with invalid order_date.")
        df.dropna(subset=["order_date"], inplace=True)

    # ── 4. Cast numeric columns ───────────────────────────────
    for col in NUMERIC_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    invalid_num = df[NUMERIC_COLS].isna().any(axis=1).sum()
    if invalid_num:
        logger.warning(f"Dropping {invalid_num} rows with non-numeric quantity/price/cost.")
        df.dropna(subset=NUMERIC_COLS, inplace=True)

    # ── 5. Business rule validations ──────────────────────────
    df = df[df["quantity"] > 0]
    df = df[df["unit_price"] > 0]
    df = df[df["unit_cost"] >= 0]

    # ── 6. Cast order_id to int ───────────────────────────────
    df["order_id"] = df["order_id"].astype(int)

    logger.info(f"{len(df):,} rows remain after cleaning.")

    # ── Build dim_customer ────────────────────────────────────
    dim_customer = (
        df[["customer_id", "customer_name", "city", "country"]]
        .drop_duplicates(subset=["customer_id"])
        .copy()
    )

    # ── Build dim_product ─────────────────────────────────────
    # Use the latest unit_cost seen for each product
    dim_product = (
        df.sort_values("order_date")
        .drop_duplicates(subset=["product_id"], keep="last")
        [["product_id", "product_name", "category", "unit_cost"]]
        .copy()
    )

    # ── Build dim_date ────────────────────────────────────────
    unique_dates = df["order_date"].dt.date.unique()
    date_rows = []
    for d in unique_dates:
        ts = pd.Timestamp(d)
        date_rows.append({
            "date_id":    d,
            "day":        ts.day,
            "month":      ts.month,
            "month_name": ts.strftime("%B"),
            "quarter":    ts.quarter,
            "year":       ts.year,
            "day_of_week": ts.dayofweek,  # 0=Monday
            "day_name":   ts.strftime("%A"),
            "is_weekend": ts.dayofweek >= 5,
        })
    dim_date = pd.DataFrame(date_rows)

    # ── Build fact_sales ──────────────────────────────────────
    fact_sales = df[[
        "order_id", "order_date", "customer_id",
        "product_id", "quantity", "unit_price", "unit_cost",
    ]].copy()
    fact_sales["order_date"] = fact_sales["order_date"].dt.date

    logger.info(
        f"Transformation complete | "
        f"customers={len(dim_customer)} | "
        f"products={len(dim_product)} | "
        f"dates={len(dim_date)} | "
        f"facts={len(fact_sales)}"
    )

    return dim_customer, dim_product, dim_date, fact_sales
