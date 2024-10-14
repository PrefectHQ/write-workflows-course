from datetime import datetime, timedelta
import duckdb
import pandas as pd
from prefect import flow, task
import random


@task
def extract():
    """
    Extract sample maritime transaction data from a CSV file.
    """
    print("Extracting data...")

    df = pd.read_csv(
        "https://raw.githubusercontent.com/PrefectHQ/write-workflows-course/refs/heads/main/data/raw_maritime_transactions_2024-10-14.csv"
    )

    return df


@task
def transform(df: pd.DataFrame):
    """
    Transform the extracted data.
    """
    print("Transforming data...")

    # Currency conversion rates
    rates = {"EUR": 1.1, "USD": 1.0, "GBP": 1.3, "JPY": 0.009, "CNY": 0.15}

    # Convert all amounts to USD
    df["amount_usd"] = df.apply(
        lambda row: row["transaction_amount"] * rates[row["currency"]], axis=1
    )

    print(df.head())
    return df


@task
def load(df: pd.DataFrame):
    """
    Load the transformed data into a DuckDB database.
    """
    print("Loading data into DuckDB database")

    # Connect to db
    conn = duckdb.connect("maritime_transactions.db")

    try:
        # Insert data into the table
        conn.execute("INSERT INTO maritime_transactions SELECT * FROM df")

        # Commit the transaction
        conn.commit()

        print("Data successfully loaded into DuckDB")
    except Exception as e:
        print(f"An error occurred while loading data: {e}")
    finally:
        conn.close()


@flow(log_prints=True, name="ETL Flow")
def etl_flow():
    """
    Main flow that orchestrates the extract, transform, and load tasks.
    """
    raw_data = extract()
    transformed_data = transform(raw_data)
    load(transformed_data)
    print("ETL process completed.")


if __name__ == "__main__":
    etl_flow()
