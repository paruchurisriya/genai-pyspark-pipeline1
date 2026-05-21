"""
Benchmark: Pandas vs PySpark on join + aggregation for 1M orders

Steps:
1. Load `orders.parquet` and `products.parquet` with Pandas and PySpark
2. Join on `product_id` and compute `revenue = quantity * price`
3. Group by `customer_id`, sum revenue, get top 10 customers
4. Time each operation and print a comparison table

Usage:
    python benchmark_pandas_vs_pyspark.py

Notes:
- Ensure `pyarrow` is installed for Pandas Parquet I/O.
- The script forces Spark evaluations with `.count()` and `.collect()` to measure real work.
"""
from __future__ import annotations

import time
import sys
from pathlib import Path
from typing import Dict, Any

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def run_pandas_benchmark(orders_path: str, products_path: str) -> Dict[str, Any]:
    """Run benchmark steps using Pandas.

    Returns a dict with timings and the top-10 result (as pandas DataFrame).
    """
    metrics: Dict[str, Any] = {}

    # Load
    t0 = time.perf_counter()
    orders = pd.read_parquet(orders_path)
    products = pd.read_parquet(products_path)
    t1 = time.perf_counter()
    metrics['pandas_load_s'] = t1 - t0

    # Join + compute revenue + aggregate
    t2 = time.perf_counter()
    merged = orders.merge(products[['product_id', 'price']], on='product_id', how='left')
    merged['revenue'] = merged['quantity'] * merged['price']
    grouped = merged.groupby('customer_id', as_index=False)['revenue'].sum()
    top10 = grouped.nlargest(10, 'revenue')
    t3 = time.perf_counter()
    metrics['pandas_join_agg_s'] = t3 - t2

    metrics['pandas_total_s'] = (t3 - t0)
    metrics['pandas_top10'] = top10.reset_index(drop=True)
    return metrics


def create_spark_session_local(app_name: str = 'BenchmarkSpark') -> SparkSession:
    """Create a local SparkSession optimized for a laptop (copy-paste config)."""
    spark = SparkSession.builder \
        .appName(app_name) \
        .master('local[8]') \
        .config('spark.driver.memory', '6g') \
        .config('spark.executor.memory', '2g') \
        .config('spark.sql.shuffle.partitions', '64') \
        .config('spark.sql.adaptive.enabled', 'true') \
        .config('spark.sql.adaptive.coalescePartitions.enabled', 'true') \
        .config('spark.serializer', 'org.apache.spark.serializer.JavaSerializer') \
        .config('spark.io.compression.codec', 'snappy') \
        .getOrCreate()
    return spark


def run_spark_benchmark(orders_path: str, products_path: str) -> Dict[str, Any]:
    """Run benchmark steps using PySpark.

    Returns a dict with timings and the top-10 result (as list of tuples).
    """
    metrics: Dict[str, Any] = {}
    spark = create_spark_session_local('PandasVsSparkBenchmark')

    # Load (force evaluation via count)
    t0 = time.perf_counter()
    orders_df = spark.read.parquet(orders_path)
    products_df = spark.read.parquet(products_path)
    # Force read
    orders_count = orders_df.count()
    products_count = products_df.count()
    t1 = time.perf_counter()
    metrics['spark_load_s'] = t1 - t0
    metrics['spark_counts'] = {'orders': orders_count, 'products': products_count}

    # Join + compute revenue + aggregate (force evaluation via collect)
    t2 = time.perf_counter()
    joined = orders_df.join(products_df.select('product_id', 'price'), on='product_id', how='left')
    joined = joined.withColumn('revenue', F.col('quantity') * F.col('price'))
    grouped = (joined.groupBy('customer_id')
               .agg(F.sum('revenue').alias('total_revenue'))
               .orderBy(F.desc('total_revenue'))
               .limit(10))
    top10 = grouped.collect()
    t3 = time.perf_counter()
    metrics['spark_join_agg_s'] = t3 - t2

    metrics['spark_total_s'] = (t3 - t0)
    # Convert top10 to Python list of dicts for display
    metrics['spark_top10'] = [
        {'customer_id': row['customer_id'], 'total_revenue': float(row['total_revenue'])}
        for row in top10
    ]

    # Stop spark
    spark.stop()
    return metrics


def print_comparison(pandas_m: Dict[str, Any], spark_m: Dict[str, Any]) -> None:
    """Print a simple comparison table of timings."""
    headers = ['Step', 'Pandas (s)', 'PySpark (s)', 'Faster']
    rows = []

    rows.append(('Load', f"{pandas_m['pandas_load_s']:.3f}", f"{spark_m['spark_load_s']:.3f}",
                 'Pandas' if pandas_m['pandas_load_s'] < spark_m['spark_load_s'] else 'PySpark'))
    rows.append(('Join+Agg', f"{pandas_m['pandas_join_agg_s']:.3f}", f"{spark_m['spark_join_agg_s']:.3f}",
                 'Pandas' if pandas_m['pandas_join_agg_s'] < spark_m['spark_join_agg_s'] else 'PySpark'))
    rows.append(('Total', f"{pandas_m['pandas_total_s']:.3f}", f"{spark_m['spark_total_s']:.3f}",
                 'Pandas' if pandas_m['pandas_total_s'] < spark_m['spark_total_s'] else 'PySpark'))

    # Print table
    col_widths = [20, 15, 15, 10]
    fmt = ''.join(f'{{:<{w}}}' for w in col_widths)
    print('\n' + fmt.format(*headers))
    print('-' * sum(col_widths))
    for r in rows:
        print(fmt.format(*r))

    print('\nTop 10 (Pandas):')
    print(pandas_m['pandas_top10'].to_string(index=False))

    print('\nTop 10 (PySpark):')
    for i, rec in enumerate(spark_m['spark_top10'], start=1):
        print(f"{i:2d}. customer_id={rec['customer_id']}, total_revenue={rec['total_revenue']:.2f}")


def main() -> None:
    base = Path('data/raw')
    orders_path = str(base / 'orders.parquet')
    products_path = str(base / 'products.parquet')

    print('Running Pandas benchmark...')
    pandas_metrics = run_pandas_benchmark(orders_path, products_path)

    print('Running PySpark benchmark...')
    spark_metrics = run_spark_benchmark(orders_path, products_path)

    print_comparison(pandas_metrics, spark_metrics)


if __name__ == '__main__':
    main()
