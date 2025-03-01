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
        entrypoint="path/to/my_remote_flow_code_file.py:entrypoint function",
    ).serve(
        name="fetch-and-save-snowflake-stock-data-from-private-gh-repo",
        cron="0 0 * * *",
        parameters={"ticker": "SNOW"},
    )
