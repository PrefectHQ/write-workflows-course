import random
import pandas as pd
import yfinance as yf
from prefect import flow, task
from prefect.tasks import exponential_backoff

def retry_handler(task, task_run, state) -> bool:
    """Custom retry handler that specifies when to retry a task"""
    try:
        # Attempt to get the result of the task
        state.result()
    except httpx.HTTPStatusError as exc:
        # Retry on any HTTP status code that is not 401 or 404
        do_not_retry_on_this_code = [404]
        return exc.response.status_code not in do_not_retry_on_this_code
    except httpx.ConnectError:
        # Do not retry on connection error
        return False
    except:
        # Retry on any other exception
        return True


@task(
    retries=2, retry_condition_fn=retry_handler
)  # retry_delay_seconds=[2, 3] # retry_delay_seconds=[2, 3] # retry_delay_seconds=exponential_backoff(backoff_factor=5) # retry_jitter_factor=1
# retry behavior globally
# prefect config set PREFECT_TASK_DEFAULT_RETRIES=2

# def retry_handler(task, task_run, state) -> bool:
#     """Custom retry handler that specifies when to retry a task"""
#     try:
#         # Attempt to get the result of the task
#         state.result()
#     except httpx.HTTPStatusError as exc:
#         # Retry on any HTTP status code that is not 401 or 404
#         do_not_retry_on_these_codes = [401, 404]
#         return exc.response.status_code not in do_not_retry_on_these_codes
#     except httpx.ConnectError:
#         # Do not retry
#         return False
#     except:
#         # For any other exception, retry
#         return True


# @task(retries=1, retry_condition_fn=retry_handler)


def fetch_stock_data(
    ticker: str, start_date: str, end_date: str, period: str = "1d"
) -> pd.DataFrame:
    if random.random() < 0.7:
        raise Exception("This is an error simulating an intermittent API failure")
    df = yf.download(ticker, start=start_date, end=end_date, period=period)

    return df


@task
def save_raw_stock_data(df: pd.DataFrame, filename: str):
    df.to_csv(filename)


@task
def transform_stock_data(df: pd.DataFrame) -> pd.DataFrame:
    compute_moving_average = df["Close"].rolling(window=3).mean()
    df["Moving Average"] = compute_moving_average
    return df


@task
def save_transformed_stock_data(df: pd.DataFrame, filename: str):
    df.to_csv(filename)


@flow(log_prints=True)
def fetch_and_save_stock_data(
    ticker: str = "AAPL",
    start_date: str = "2025-02-01",
    end_date: str = "2025-02-28",
    period: str = "1d",
):
    df = fetch_stock_data(ticker, start_date, end_date, period)
    df.columns = ["_".join(col).strip() for col in df.columns.values]
    print(df)
    save_stock_data(df, f"{ticker}_stock_data.csv")


if __name__ == "__main__":
    # fetch_and_save_stock_data("MSFT")
    fetch_and_save_stock_data.serve(
        name="fetch-and-save-stock-data",
        # cron="0 0 * * *",
    )
    # If you change the flow entrypoint parameters, you must restart the process. so control c and rerun the script

    fetch_and_save_stock_data.from_source(
        # get the source code from GitHub
         source="https://github.com/org/repo.git",
        entrypoint="path/to/my_remote_flow_code_file.py:say_hi",
        # source=Source.from_github(
        #     repo="Prefect-Community/prefect-community",
        #     path="solutions/self-serve/write_workflows/03_start_observing/solution_stock_data.py",
        ),
    ).serve(
        name="fetch-and-save-stock-data-",
        # cron="0 0 * * *",
        parameters={
            "ticker": "GOOG",
            "start_date": "2025-01-01",
            "end_date": "2025-01-28",
            "period": "1d",
        },
    )


# private GH repo:
# or just see the docs with examples of many types of flow code storage. The samples show using.deploy, but they are similar with .serve
from prefect import flow
from prefect.runner.storage import GitRepository
from prefect.blocks.system import Secret


if __name__ == "__main__":

    github_repo = GitRepository(
        url="https://github.com/org/my-private-repo.git",
        credentials={
            "access_token": Secret.load("my-secret-block-with-my-gh-credentials")
        },
    )

    flow.from_source(
        source=github_repo,
        entrypoint="gh_secret_block.py:my_flow",
    ).deploy(
        name="private-github-deploy",
        work_pool_name="my_pool",
    )