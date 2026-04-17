"""
Configuration file for Binance GCP Loader
Edit these values to match your environment
"""

# GCP Cloud SQL Connection Details
GCP_CONFIG = {
    'host': 'your host here',
    'username': 'your username here',
    'password': 'your password here',
    'database': 'your db here',  # Change this if you created a different database
    'port': 5432
}

# File Locations
PATHS = {

    'parquet_directory': r'/mnt/d/DVA Project/Datasets/Binance Full History',
   
}

# Loading Parameters
LOAD_CONFIG = {
    'table_name': 'ohlc_data',
    'chunk_size': 10000,  # Rows per batch insert
    'file_limit': None,   # Set to a number to test with limited files, None to load all
    'file_pattern': '*.parquet'
}

# Database Schema
SCHEMA_CONFIG = {
    'table_name': 'ohlc_data',
    'drop_existing': True,  # WARNING: Set to False to preserve existing data
}
