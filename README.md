# genai-pyspark-pipeline1
Synthetic data generation and Pyspark Analytics for E-commerce insights.

## Project Purpose
This project generates synthetic e-commerce data (customers, products, and orders) to simulate a production environment and performs distributed data analysis using PySpark to extract business-critical insights.

## Structure
- `src/`: Core logic for data generation and analysis.
- `data/raw/`: Storage for generated CSV files.
- `data/processed/`: Storage for analysis results (Parquet/CSV).
- `tests/`: Unit tests for the pipeline components.

## Setup
1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Generate data:
   ```bash
   python src/data_generator.py
   ```
3. Run analytics:
   ```bash
   python src/spark_analytics.py
   ```
