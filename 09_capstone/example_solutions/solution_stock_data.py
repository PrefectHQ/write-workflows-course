import pandas as pd
import yfinance as yf
from prefect import flow, task


@task(retries=2, retry_delay_seconds=4)
def fetch_stock_data(
    ticker: str, start_date: str, end_date: str, period: str = "1d"
) -> pd.DataFrame:
    df = yf.download(ticker, start=start_date, end=end_date, period=period)
    return df


@task
def save_stock_data(df: pd.DataFrame, filename: str):
    df.to_csv(filename)


@flow(log_prints=True)
def fetch_and_save_stock_data(
    ticker: str = "AAPL",
    start_date: str = "2025-02-01",
    end_date: str = "2025-02-26",
    period: str = "1d",
):
    df = fetch_stock_data(ticker, start_date, end_date, period)
    df.columns = ["_".join(col).strip() for col in df.columns.values]

    print(df)
    save_stock_data(df, f"{ticker}_stock_data.csv")


if __name__ == "__main__":
    fetch_and_save_stock_data()
    # fetch_and_save_stock_data.serve(
    # name="fetch-and-save-stock-data",
    # cron="0 0 * * *",
    # parameters={
    #     "ticker": "AAPL",
    #     "start_date": "2025-02-01",
    #     "end_date": "2025-02-26",
    #     "period": "1d",
    # },
#  )
