import random
import pandas as pd
import yfinance as yf
from prefect import flow, task
from prefect.schedules import Cron, Interval
from datetime import timedelta


@task(retries=2)
def fetch_stock_data(
    ticker: str, start_date: str, end_date: str, period: str = "1d"
) -> pd.DataFrame:
    """Fetch the stock data from Yahoo Finance."""
    df = yf.download(ticker, start=start_date, end=end_date, period=period)
    return df


@task
def save_raw_stock_data(df: pd.DataFrame, filename: str):
    """Save the raw stock data to a CSV file."""
    df.to_csv(f"./data/{filename}")


@task
def transform_stock_data(df: pd.DataFrame) -> pd.DataFrame:
    """Compute the moving average of the close price for the previous 3 days."""
    stock_name = df.columns.get_level_values(1)[0]
    df[("Moving Average Close", stock_name)] = df["Close"].rolling(window=3).mean()
    return df


@task
def save_transformed_stock_data(df: pd.DataFrame, filename: str):
    """Write the transformed stock data to a CSV file."""
    df.to_csv(f"./data/{filename}")
    print(f"Saved transformed stock data to ./data/{filename}")


@flow(log_prints=True)
def fetch_and_save_stock_data(
    ticker: str = "AAPL",
    start_date: str = "2025-02-01",
    end_date: str = "2025-02-28",
    period: str = "1d",
):
    """Main ETL workflow to fetch and save stock data."""
    df_raw = fetch_stock_data(ticker, start_date, end_date, period)
    save_raw_stock_data(df_raw, f"{ticker}_stock_data.csv")
    df_transformed = transform_stock_data(df_raw)
    save_transformed_stock_data(df_transformed, f"{ticker}_transformed_stock_data.csv")
    print(df_transformed)


if __name__ == "__main__":
    fetch_and_save_stock_data.serve(
        name="fetch-and-save-snowflake-stock-data",
        schedules=[
            Cron(
                "1 1 1 1 1",
                timezone="America/New_York",
            ),
            Interval(
                timedelta(days=1),
                timezone="America/New_York",
            ),
        ],
    )
