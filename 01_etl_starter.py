from datetime import datetime, timedelta
import duckdb
import pandas as pd
import random


def extract(date_to_fetch: str):
    """Extract sample maritime transaction data from CSV file."""

    try:
        url = f"https://raw.githubusercontent.com/PrefectHQ/write-workflows-course/refs/heads/main/data/maritime_transactions_{date_to_fetch}.csv"

        df = pd.read_csv(url)
        print(f"Raw data: {df.head()}")
        return df
    except Exception as e:
        print(f"An error occurred while extracting data: {e}")


def transform(df: pd.DataFrame):
    """Transform extracted data."""

    try:
        rates = {"EUR": 1.1, "USD": 1.0, "GBP": 1.3, "JPY": 0.009, "CNY": 0.15}
        df["amount_usd"] = df.apply(
            lambda row: row["transaction_amount"] * rates[row["currency"]], axis=1
        )
        print(f"Transformed data: {df.head()}")
        return df
    except Exception as e:
        print(f"An error occurred while transforming data: {e}")


def load(df: pd.DataFrame):
    """Load transformed data into DuckDB database."""

    try:
        conn = duckdb.connect("maritime_transactions.db")
        conn.execute("INSERT INTO maritime_transactions SELECT * FROM df")
        conn.commit()
    except Exception as e:
        print(f"An error occurred while loading data: {e}")
    finally:
        conn.close()


def etl(date_to_fetch: str):
    """Main flow that orchestrates the extract, transform, and load tasks."""

    raw_data = extract(date_to_fetch=date_to_fetch)
    transformed_data = transform(raw_data)
    load(transformed_data)
    print("ETL process completed.")


if __name__ == "__main__":
    etl(date_to_fetch="2024-10-05")
