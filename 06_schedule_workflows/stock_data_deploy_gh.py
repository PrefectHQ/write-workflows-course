from prefect import flow


if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/PrefectHQ/write-workflows-course.git",
        entrypoint="03_start_observing/stock_data_flow.py:stock_data_flow",
    ).serve(
        name="stock-data-from-gh-repo",
        cron="0 0 0 * *",
    )
