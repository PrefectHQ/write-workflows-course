from prefect import flow, task
import pandas as pd
import duckdb
from datetime import datetime, timedelta
import random

@task
def extract():
    """
    Extract sample maritime transaction data.
    In a real scenario, this might involve API calls or database queries.
    """
    print("Extracting data...")
    data = []
    for _ in range(100):
        data.append({
            'transaction_id': f'TRX{random.randint(1000, 9999)}',
            'ship_name': f'SHIP{random.randint(1, 50)}',
            'transaction_amount': round(random.uniform(1000, 100000), 2),
            'transaction_date': (datetime.now() - timedelta(days=random.randint(0, 30))).strftime('%Y-%m-%d'),
            'port': random.choice(['Rotterdam', 'Singapore', 'Shanghai', 'Los Angeles', 'New York']),
            'currency': random.choice(['EUR', 'USD', 'GBP', 'JPY', 'CNY'])
        })
    return pd.DataFrame(data)

@task
def transform(df: pd.DataFrame):
    """
    Transform the extracted data.
    Perform currency conversion, data type changes, and calculate additional metrics.
    """
    print("Transforming data...")
    
    # Currency conversion rates (simplified for demonstration)
    rates = {'EUR': 1.1, 'USD': 1.0, 'GBP': 1.3, 'JPY': 0.009, 'CNY': 0.15}
    
    # Convert all amounts to USD
    df['amount_usd'] = df.apply(lambda row: row['transaction_amount'] * rates[row['currency']], axis=1)
    
    # Convert date string to datetime
    df['transaction_date'] = pd.to_datetime(df['transaction_date'])
    
    # Categorize transactions
    df['transaction_category'] = pd.cut(df['amount_usd'], 
                                        bins=[0, 10000, 50000, float('inf')],
                                        labels=['Small', 'Medium', 'Large'])
    
    # Calculate total transaction value per ship
    ship_totals = df.groupby('ship_name')['amount_usd'].sum().reset_index()
    ship_totals.columns = ['ship_name', 'total_transaction_value']
    df = df.merge(ship_totals, on='ship_name', how='left')
    
    # Identify busiest ports
    port_counts = df['port'].value_counts().reset_index()
    port_counts.columns = ['port', 'transaction_count']
    df = df.merge(port_counts, on='port', how='left')
    
    return df

@task
def load(df: pd.DataFrame):
    """
    Load the transformed data into a DuckDB database.
    """
    print("Loading data into DuckDB...")
    
    # Connect to DuckDB (this will create a new database if it doesn't exist)
    conn = duckdb.connect('maritime_transactions.db')
    
    try:
        # Create table if it doesn't exist
        conn.execute("""
            CREATE TABLE IF NOT EXISTS maritime_transactions (
                transaction_id VARCHAR,
                ship_name VARCHAR,
                transaction_amount FLOAT,
                transaction_date DATE,
                port VARCHAR,
                currency VARCHAR,
                amount_usd FLOAT,
                transaction_category VARCHAR,
                total_transaction_value FLOAT,
                transaction_count INTEGER
            )
        """)
        
        # Insert data into the table
        conn.execute("INSERT INTO maritime_transactions SELECT * FROM df")
        
        # Commit the transaction
        conn.commit()
        
        print("Data successfully loaded into DuckDB")
    except Exception as e:
        print(f"An error occurred while loading data: {e}")
    finally:
        conn.close()

@flow
def etl_flow():
    """
    Main ETL flow that orchestrates the extract, transform, and load tasks.
    """
    raw_data = extract()
    transformed_data = transform(raw_data)
    load(transformed_data)
    print("ETL process completed.")

if __name__ == "__main__":
    etl_flow()
