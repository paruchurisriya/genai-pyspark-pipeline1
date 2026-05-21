import os
from pathlib import Path

# Project Root
BASE_DIR = Path(__file__).resolve().parent

# Data Directories
RAW_DATA_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DATA_DIR = BASE_DIR / "data" / "processed"

# Create directories if they don't exist
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)

# Configuration Settings
NUM_CUSTOMERS = 100000
NUM_PRODUCTS = 10000
NUM_ORDERS = 1000000

# File Paths
CUSTOMERS_FILE = RAW_DATA_DIR / "customers.csv"
PRODUCTS_FILE = RAW_DATA_DIR / "products.csv"
ORDERS_FILE = RAW_DATA_DIR / "orders.csv"

CUSTOMERS_PARQUET = RAW_DATA_DIR / "customers.parquet"
PRODUCTS_PARQUET = RAW_DATA_DIR / "products.parquet"
ORDERS_PARQUET = RAW_DATA_DIR / "orders.parquet"