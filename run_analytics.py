"""
Analytics execution script for e-commerce data analysis.

This script demonstrates the usage of the SalesAnalytics class by:
- Loading customer, product, and order data from Parquet files
- Running all three analytics methods
- Displaying results with execution time metrics
"""

import time
import logging
import warnings
from pathlib import Path

# Suppress Spark warnings
warnings.filterwarnings('ignore')
import os
os.environ['PYSPARK_PYTHON'] = 'python'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'python'

from spark_analytics import SalesAnalytics

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def print_execution_header(title: str) -> None:
    """Print a formatted header for section output."""
    print("\n" + "=" * 100)
    print(f"  {title}")
    print("=" * 100 + "\n")


def run_analytics() -> None:
    """
    Execute all analytics operations and display results with timing metrics.
    
    Raises:
        Exception: If any analytics operation fails.
    """
    analytics = SalesAnalytics()
    overall_start_time = time.time()
    
    try:
        # Define data paths
        data_dir = Path("data/raw")
        customers_path = str(data_dir / "customers.parquet")
        products_path = str(data_dir / "products.parquet")
        orders_path = str(data_dir / "orders.parquet")
        
        # Step 1: Load data with timing
        print_execution_header("STEP 1: LOADING DATA")
        
        load_start = time.time()
        logger.info(f"Loading customers from: {customers_path}")
        customers_df = analytics.load_parquet(customers_path)
        customers_time = time.time() - load_start
        print(f"[OK] Customers loaded in {customers_time:.3f}s ({customers_df.count():,} records)\n")
        
        load_start = time.time()
        logger.info(f"Loading products from: {products_path}")
        products_df = analytics.load_parquet(products_path)
        products_time = time.time() - load_start
        print(f"[OK] Products loaded in {products_time:.3f}s ({products_df.count():,} records)\n")
        
        load_start = time.time()
        logger.info(f"Loading orders from: {orders_path}")
        orders_df = analytics.load_parquet(orders_path)
        orders_time = time.time() - load_start
        print(f"[OK] Orders loaded in {orders_time:.3f}s ({orders_df.count():,} records)\n")
        
        total_load_time = customers_time + products_time + orders_time
        print(f"Total Data Loading Time: {total_load_time:.3f}s\n")
        
        # Step 2: Display data schemas
        print_execution_header("STEP 2: DATA SCHEMAS")
        
        print("Customers Schema:")
        customers_df.printSchema()
        print()
        
        print("Products Schema:")
        products_df.printSchema()
        print()
        
        print("Orders Schema:")
        orders_df.printSchema()
        print()
        
        # Step 3: Top Customers Analysis
        print_execution_header("STEP 3: TOP 10 CUSTOMERS BY REVENUE")
        
        analysis_start = time.time()
        logger.info("Starting top customers analysis...")
        top_customers = analytics.top_customers_by_revenue(orders_df, products_df, n=10)
        # Trigger evaluation with show()
        top_customers.show(truncate=False)
        top_customers_time = time.time() - analysis_start
        
        print(f"\nExecution Time: {top_customers_time:.3f}s\n")
        
        # Step 4: Sales by Category Analysis
        print_execution_header("STEP 4: SALES BY CATEGORY")
        
        analysis_start = time.time()
        logger.info("Starting category sales analysis...")
        category_sales = analytics.sales_by_category(orders_df, products_df)
        # Trigger evaluation with show()
        category_sales.show(truncate=False)
        category_sales_time = time.time() - analysis_start
        
        print(f"\nExecution Time: {category_sales_time:.3f}s\n")
        
        # Step 5: Monthly Trends Analysis
        print_execution_header("STEP 5: MONTHLY REVENUE TRENDS")
        
        analysis_start = time.time()
        logger.info("Starting monthly trends analysis...")
        monthly_trends = analytics.monthly_trends(orders_df, products_df)
        # Trigger evaluation with show()
        monthly_trends.show(truncate=False)
        monthly_trends_time = time.time() - analysis_start
        
        print(f"\nExecution Time: {monthly_trends_time:.3f}s\n")
        
        # Summary metrics
        print_execution_header("EXECUTION SUMMARY")
        
        print("Operation Timing Breakdown:")
        print(f"  - Data Loading (Total):        {total_load_time:>8.3f}s")
        print(f"    * Customers:                 {customers_time:>8.3f}s")
        print(f"    * Products:                  {products_time:>8.3f}s")
        print(f"    * Orders:                    {orders_time:>8.3f}s")
        print()
        print(f"  - Top Customers Analysis:      {top_customers_time:>8.3f}s")
        print(f"  - Category Sales Analysis:     {category_sales_time:>8.3f}s")
        print(f"  - Monthly Trends Analysis:     {monthly_trends_time:>8.3f}s")
        print()
        
        total_analysis_time = top_customers_time + category_sales_time + monthly_trends_time
        print(f"  - Total Analysis Time:         {total_analysis_time:>8.3f}s")
        
        overall_time = time.time() - overall_start_time
        print(f"  - Total Execution Time:        {overall_time:>8.3f}s")
        print()
        
        # Data insights
        print("Data Insights:")
        print(f"  - Total Customers:             {customers_df.count():>8,}")
        print(f"  - Total Products:              {products_df.count():>8,}")
        print(f"  - Total Orders:                {orders_df.count():>8,}")
        print()
        
        logger.info(f"[OK] All analytics operations completed successfully in {overall_time:.3f}s")
        
    except Exception as e:
        logger.error(f"Error during analytics execution: {str(e)}", exc_info=True)
        raise
    finally:
        # Stop Spark session
        print_execution_header("CLEANUP")
        logger.info("Stopping Spark session...")
        try:
            analytics.spark.stop()
            logger.info("[OK] Spark session stopped successfully")
        except:
            pass
        print()


if __name__ == "__main__":
    print("\n")
    print("=" * 100)
    print(" " * 30 + "E-COMMERCE ANALYTICS PIPELINE")
    print("=" * 100)
    print()
    
    run_analytics()
    
    print("=" * 100)
    print(" " * 35 + "ANALYTICS COMPLETED")
    print("=" * 100)
    print()

