import logging
import random
import numpy as np
import pandas as pd
from faker import Faker
from tqdm import tqdm
from typing import List, Optional
from config import CUSTOMERS_FILE, PRODUCTS_FILE, ORDERS_FILE, NUM_CUSTOMERS, NUM_PRODUCTS, NUM_ORDERS

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SyntheticDataGenerator:
    """
    A class to generate synthetic e-commerce data including customers, 
    products, and orders using Faker and NumPy distributions.
    """

    def __init__(self, locale: str = 'en_US'):
        """
        Initializes the generator with a Faker instance.

        Args:
            locale: Locale for Faker data generation.
        """
        self.fake = Faker(locale)
        logger.info(f"Initialized SyntheticDataGenerator with locale: {locale}")

    def generate_customers(self, num_records: int) -> pd.DataFrame:
        """
        Generates synthetic customer data with age following a normal distribution.

        Args:
            num_records: The number of customer records to create.

        Returns:
            A pandas DataFrame containing customer details.
        """
        logger.info(f"Generating {num_records} customers...")
        
        # Age distribution: Normal around 35, standard deviation of 12
        ages = np.random.normal(loc=35, scale=12, size=num_records).astype(int)
        ages = np.clip(ages, 18, 95)

        customers = []
        for i in tqdm(range(num_records), desc="Generating Customers"):
            customers.append({
                "customer_id": i + 1,
                "name": self.fake.name(),
                "email": self.fake.email(),
                "age": int(ages[i]),
                "city": self.fake.city(),
                "country": self.fake.country(),
                "registration_date": self.fake.date_between(start_date='-5y', end_date='today')
            })
        return pd.DataFrame(customers)

    def generate_products(self, num_records: int) -> pd.DataFrame:
        """
        Generates synthetic product data.

        Args:
            num_records: The number of product records to create.

        Returns:
            A pandas DataFrame containing product details.
        """
        logger.info(f"Generating {num_records} products...")
        categories = ['Electronics', 'Clothing', 'Home', 'Sports', 'Books']
        
        products = []
        for i in tqdm(range(num_records), desc="Generating Products"):
            products.append({
                "product_id": i + 1,
                "name": self.fake.catch_phrase(),
                "category": random.choice(categories),
                "price": round(random.uniform(10.0, 500.0), 2),
                "stock": random.randint(0, 1000),
                "rating": round(random.uniform(1.0, 5.0), 1)
            })
        return pd.DataFrame(products)

    def generate_orders(self, num_records: int, customer_ids: List[int], product_ids: List[int]) -> pd.DataFrame:
        """
        Generates synthetic order data using a Pareto distribution for customer selection
        to simulate the 80/20 rule (20% of customers make 80% of orders).

        Args:
            num_records: The number of orders to generate.
            customer_ids: List of available customer IDs.
            product_ids: List of available product IDs.

        Returns:
            A pandas DataFrame containing order details.
        """
        logger.info(f"Generating {num_records} orders...")
        
        # Generate weights using Pareto distribution (alpha ~ 1.16 approximates 80/20)
        weights = np.random.pareto(1.16, len(customer_ids)) + 1
        weights /= weights.sum()
        
        # Pre-sample data for efficiency
        sampled_customers = np.random.choice(customer_ids, size=num_records, p=weights)
        sampled_products = np.random.choice(product_ids, size=num_records)
        sampled_quantities = np.random.randint(1, 11, size=num_records)

        orders = []
        for i in tqdm(range(num_records), desc="Generating Orders"):
            orders.append({
                "order_id": i + 1,
                "customer_id": int(sampled_customers[i]),
                "product_id": int(sampled_products[i]),
                "quantity": int(sampled_quantities[i]),
                "order_date": self.fake.date_between(start_date='-2y', end_date='today')
            })
        return pd.DataFrame(orders)

def main() -> None:
    """Main execution function to generate and save datasets."""
    generator = SyntheticDataGenerator()
    
    customers_df = generator.generate_customers(NUM_CUSTOMERS)
    products_df = generator.generate_products(NUM_PRODUCTS)
    orders_df = generator.generate_orders(
        NUM_ORDERS, 
        customers_df['customer_id'].tolist(), 
        products_df['product_id'].tolist()
    )

    logger.info("Saving data to CSV...")
    customers_df.to_csv(CUSTOMERS_FILE, index=False)
    products_df.to_csv(PRODUCTS_FILE, index=False)
    orders_df.to_csv(ORDERS_FILE, index=False)
    
    logger.info(f"Successfully saved datasets to {CUSTOMERS_FILE.parent}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Data generation process failed: {e}", exc_info=True)