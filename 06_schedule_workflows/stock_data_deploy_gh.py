from prefect import flow


if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/PrefectHQ/write-workflows-course.git",
        entrypoint="03_start_observing/stock_data_flow.py:fetch_and_save_stock_data",
    ).serve(
        name="stock-data-from-gh-repo",
        cron="1 1 1 1 1",
    )
