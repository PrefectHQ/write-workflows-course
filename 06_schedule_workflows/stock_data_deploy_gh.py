from prefect import flow


if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/org/repo.git",  # TK update
        entrypoint="path/to/my_remote_flow_code_file.py:entrypoint function",
    ).serve(
        name="fetch-and-save-snowflake-stock-data",
        cron="0 0 * * *",
    )
