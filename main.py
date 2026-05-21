import time
import os
import logging
from pathlib import Path
from data_generator import SyntheticDataGenerator
import config

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def format_file_size(file_path: Path) -> str:
    """Calculates file size and returns a formatted string in MB."""
    try:
        size_bytes = os.path.getsize(file_path)
        return f"{size_bytes / (1024 * 1024):.2f} MB"
    except OSError:
        return "Unknown"

def main():
    """Main orchestrator for generating and saving e-commerce data as Parquet."""
    try:
        logger.info("Initializing Synthetic Data Pipeline...")
        start_time = time.time()
        
        # Initialize generator
        generator = SyntheticDataGenerator()
        
        # 1. Generate Data using counts from config
        logger.info(f"Generating {config.NUM_CUSTOMERS:,} customers...")
        customers_df = generator.generate_customers(config.NUM_CUSTOMERS)
        
        logger.info(f"Generating {config.NUM_PRODUCTS:,} products...")
        products_df = generator.generate_products(config.NUM_PRODUCTS)
        
        logger.info(f"Generating {config.NUM_ORDERS:,} orders...")
        orders_df = generator.generate_orders(
            config.NUM_ORDERS,
            customers_df['customer_id'].tolist(),
            products_df['product_id'].tolist()
        )
        
        # 2. Define Parquet file paths in data/raw/
        customers_parquet = config.RAW_DATA_DIR / "customers.parquet"
        products_parquet = config.RAW_DATA_DIR / "products.parquet"
        orders_parquet = config.RAW_DATA_DIR / "orders.parquet"
        
        # 3. Save to Parquet
        # Note: requires 'pyarrow' or 'fastparquet' installed
        logger.info("Saving datasets to Parquet format in data/raw/...")
        customers_df.to_parquet(customers_parquet, index=False)
        products_df.to_parquet(products_parquet, index=False)
        orders_df.to_parquet(orders_parquet, index=False)
        
        total_time = time.time() - start_time
        
        # 4. Print Summary Report
        print("\n" + "="*55)
        print("PIPELINE SUMMARY: SYNTHETIC DATA GENERATION")
        print("="*55)
        print(f"Total Execution Time: {total_time:.2f} seconds")
        print("-" * 55)
        print(f"{'Dataset':<15} | {'Records':<12} | {'File Size':<12}")
        print("-" * 55)
        print(f"{'Customers':<15} | {len(customers_df):<12,} | {format_file_size(customers_parquet):<12}")
        print(f"{'Products':<15} | {len(products_df):<12,} | {format_file_size(products_parquet):<12}")
        print(f"{'Orders':<15} | {len(orders_df):<12,} | {format_file_size(orders_parquet):<12}")
        print("="*55)
        logger.info("Pipeline execution finished successfully.")

    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()