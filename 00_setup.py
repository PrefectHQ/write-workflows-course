from datetime import datetime, timedelta
import duckdb
import pandas as pd
import random


def extract():
    """
    Extract sample maritime transaction data from a CSV file.
    """
    print("Extracting data...")

    df = pd.read_csv(
        "https://raw.githubusercontent.com/PrefectHQ/write-workflows-course/refs/heads/main/data/maritime_transactions_2024-10-04.csv"
    )

    return df


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


def load(df: pd.DataFrame):
    """
    Create a DuckDB database and load the data into it.
    """
    print("Loading data into DuckDB database")

    # Connect to DuckDB (this will create a new database if it doesn't exist)
    conn = duckdb.connect("maritime_transactions.db")

    try:
        # Create table if it doesn't exist
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS maritime_transactions (
                transaction_id VARCHAR,
                ship_name VARCHAR,
                transaction_amount FLOAT,
                transaction_date DATE,
                port VARCHAR,
                currency VARCHAR,
                amount_usd FLOAT,
            )
        """
        )

        # Insert data into the table
        conn.execute("INSERT INTO maritime_transactions SELECT * FROM df")

        # Commit the transaction
        conn.commit()

        print("Data successfully loaded into DuckDB")
    except Exception as e:
        print(f"An error occurred while loading data: {e}")
    finally:
        conn.close()


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
