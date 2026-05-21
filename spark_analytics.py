import logging
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from typing import Optional
from pathlib import Path
from config import CUSTOMERS_PARQUET, PRODUCTS_PARQUET, ORDERS_PARQUET, PROCESSED_DATA_DIR

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SalesAnalytics:
    """
    A comprehensive PySpark analytics class for e-commerce sales data analysis.
    
    This class provides methods to perform advanced analytics including:
    - Customer revenue analysis
    - Sales by category analysis
    - Monthly trend analysis with growth calculations
    
    Attributes:
        spark: The SparkSession instance
    """
    
    def __init__(self):
        """Initialize SalesAnalytics instance with a configured SparkSession."""
        self.spark: SparkSession = self.create_spark_session()
    
    @staticmethod
    def create_spark_session(app_name: str = "SalesAnalytics") -> SparkSession:
        """
        Create and configure a SparkSession with optimized settings.
        
        Configuration includes:
        - 2GB driver memory
        - 2GB executor memory
        - Adaptive query execution enabled
        - Java serialization for better Python 3.14 compatibility
        - Dynamic partition pruning enabled
        
        Args:
            app_name (str): Name of the Spark application. Defaults to "SalesAnalytics".
        
        Returns:
            SparkSession: Configured SparkSession instance.
        
        Example:
            >>> spark = SalesAnalytics.create_spark_session("MyAnalytics")
            >>> print(spark.version)
        """
        logger.info(f"Creating Spark session: {app_name}")
        
        spark = SparkSession.builder \
            .appName(app_name) \
            .master("local[*]") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.dynamicPartitionPruning.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer") \
            .config("spark.sql.shuffle.partitions", "100") \
            .config("spark.sql.session.timeZone", "UTC") \
            .getOrCreate()
        
        logger.info("Spark session created successfully")
        return spark
    
    def load_parquet(self, path: str) -> DataFrame:
        """
        Load a Parquet file into a Spark DataFrame.
        
        Args:
            path (str): Path to the Parquet file or directory.
        
        Returns:
            DataFrame: Loaded Spark DataFrame.
        
        Raises:
            Exception: If the file cannot be read.
        
        Example:
            >>> analytics = SalesAnalytics()
            >>> orders_df = analytics.load_parquet("data/raw/orders.parquet")
        """
        logger.info(f"Loading Parquet file from: {path}")
        try:
            df = self.spark.read.parquet(path)
            logger.info(f"Successfully loaded {df.count()} records from {path}")
            return df
        except Exception as e:
            logger.error(f"Failed to load Parquet file from {path}: {str(e)}")
            raise
    
    def top_customers_by_revenue(
        self, 
        orders_df: DataFrame, 
        products_df: DataFrame, 
        n: int = 10
    ) -> DataFrame:
        """
        Calculate top N customers by total revenue spent.
        
        This method joins orders with products, calculates total revenue per customer,
        and returns the top N customers sorted by revenue in descending order.
        
        Args:
            orders_df (DataFrame): Orders DataFrame with columns: order_id, customer_id, 
                                   product_id, quantity, order_date.
            products_df (DataFrame): Products DataFrame with columns: product_id, price, category.
            n (int): Number of top customers to return. Defaults to 10.
        
        Returns:
            DataFrame: Top N customers with columns: customer_id, total_revenue, 
                      order_count, avg_order_value.
        
        Example:
            >>> top_10 = analytics.top_customers_by_revenue(orders_df, products_df, n=10)
            >>> top_10.show()
        """
        logger.info(f"Calculating top {n} customers by revenue")
        
        # Join orders with products to get price information
        order_details = orders_df.join(
            products_df.select("product_id", "price"),
            on="product_id",
            how="inner"
        )
        
        # Calculate line revenue
        order_details = order_details.withColumn(
            "line_revenue",
            F.col("quantity") * F.col("price")
        )
        
        # Aggregate by customer
        customer_revenue = order_details.groupBy("customer_id") \
            .agg(
                F.sum("line_revenue").alias("total_revenue"),
                F.count("order_id").alias("order_count"),
                (F.sum("line_revenue") / F.count("order_id")).alias("avg_order_value")
            ) \
            .orderBy(F.desc("total_revenue")) \
            .limit(n)
        
        logger.info(f"Top {n} customers calculated successfully")
        return customer_revenue
    
    def sales_by_category(
        self, 
        orders_df: DataFrame, 
        products_df: DataFrame
    ) -> DataFrame:
        """
        Calculate total sales metrics grouped by product category.
        
        This method joins orders with products and aggregates sales metrics
        by category, including total revenue and total units sold.
        
        Args:
            orders_df (DataFrame): Orders DataFrame with columns: order_id, product_id, quantity.
            products_df (DataFrame): Products DataFrame with columns: product_id, price, category.
        
        Returns:
            DataFrame: Sales by category with columns: category, total_revenue, 
                      total_units_sold, avg_price, order_count.
        
        Example:
            >>> category_sales = analytics.sales_by_category(orders_df, products_df)
            >>> category_sales.show()
        """
        logger.info("Calculating sales metrics by category")
        
        # Join orders with products
        order_details = orders_df.join(
            products_df.select("product_id", "price", "category"),
            on="product_id",
            how="inner"
        )
        
        # Calculate line revenue
        order_details = order_details.withColumn(
            "line_revenue",
            F.col("quantity") * F.col("price")
        )
        
        # Aggregate by category
        category_sales = order_details.groupBy("category") \
            .agg(
                F.sum("line_revenue").alias("total_revenue"),
                F.sum("quantity").alias("total_units_sold"),
                F.avg("price").alias("avg_price"),
                F.count("order_id").alias("order_count")
            ) \
            .orderBy(F.desc("total_revenue"))
        
        logger.info("Category sales calculated successfully")
        return category_sales
    
    def monthly_trends(
        self, 
        orders_df: DataFrame, 
        products_df: DataFrame
    ) -> DataFrame:
        """
        Calculate month-over-month revenue growth percentage.
        
        This method uses Window functions to calculate the month-over-month revenue
        growth percentage for trend analysis.
        
        Args:
            orders_df (DataFrame): Orders DataFrame with columns: order_id, product_id, 
                                   quantity, order_date.
            products_df (DataFrame): Products DataFrame with columns: product_id, price.
        
        Returns:
            DataFrame: Monthly revenue trends with columns: year_month, total_revenue, 
                      prev_month_revenue, growth_percentage.
        
        Example:
            >>> trends = analytics.monthly_trends(orders_df, products_df)
            >>> trends.show()
        """
        logger.info("Calculating monthly revenue trends")
        
        # Join orders with products
        order_details = orders_df.join(
            products_df.select("product_id", "price"),
            on="product_id",
            how="inner"
        )
        
        # Calculate line revenue
        order_details = order_details.withColumn(
            "line_revenue",
            F.col("quantity") * F.col("price")
        )
        
        # Extract year and month
        order_details = order_details.withColumn(
            "year_month",
            F.to_date(F.trunc("order_date", "month"))
        )
        
        # Aggregate revenue by month
        monthly_revenue = order_details.groupBy("year_month") \
            .agg(F.sum("line_revenue").alias("total_revenue")) \
            .orderBy("year_month")
        
        # Define window function for previous month
        window_spec = Window.orderBy("year_month")
        
        # Calculate previous month revenue and growth percentage
        monthly_trends = monthly_revenue.withColumn(
            "prev_month_revenue",
            F.lag("total_revenue").over(window_spec)
        ).withColumn(
            "growth_percentage",
            F.when(
                F.col("prev_month_revenue").isNotNull(),
                ((F.col("total_revenue") - F.col("prev_month_revenue")) / 
                 F.col("prev_month_revenue") * 100)
            ).otherwise(None)
        )
        
        logger.info("Monthly trends calculated successfully")
        return monthly_trends


def main() -> None:
    """Main execution function demonstrating SalesAnalytics usage."""
    analytics = SalesAnalytics()
    
    try:
        # Load data
        logger.info("Loading data files...")
        customers_df = analytics.load_parquet(str(CUSTOMERS_PARQUET))
        products_df = analytics.load_parquet(str(PRODUCTS_PARQUET))
        orders_df = analytics.load_parquet(str(ORDERS_PARQUET))
        
        # Display schemas
        logger.info("Orders DataFrame schema:")
        orders_df.printSchema()
        
        # Perform analyses
        logger.info("\n" + "="*80)
        logger.info("TOP 10 CUSTOMERS BY REVENUE")
        logger.info("="*80)
        top_customers = analytics.top_customers_by_revenue(orders_df, products_df, n=10)
        top_customers.show()
        
        logger.info("\n" + "="*80)
        logger.info("SALES BY CATEGORY")
        logger.info("="*80)
        category_sales = analytics.sales_by_category(orders_df, products_df)
        category_sales.show()
        
        logger.info("\n" + "="*80)
        logger.info("MONTHLY REVENUE TRENDS")
        logger.info("="*80)
        trends = analytics.monthly_trends(orders_df, products_df)
        trends.show()
        
        # Save results
        logger.info("\nSaving results...")
        output_dir = Path(PROCESSED_DATA_DIR)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        top_customers.write.mode("overwrite").parquet(str(output_dir / "top_customers"))
        category_sales.write.mode("overwrite").parquet(str(output_dir / "category_sales"))
        trends.write.mode("overwrite").parquet(str(output_dir / "monthly_trends"))
        
        logger.info(f"Results saved to {output_dir}")
        
    except Exception as e:
        logger.error(f"Error during analysis: {str(e)}", exc_info=True)
    finally:
        analytics.spark.stop()
        logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()