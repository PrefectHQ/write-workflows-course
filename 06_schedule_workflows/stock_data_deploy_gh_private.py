from prefect import flow
from prefect.runner.storage import GitRepository
from prefect.blocks.system import Secret


if __name__ == "__main__":
    my_private_github_repo = GitRepository(
        url="https://github.com/org/my-private-repo.git",
        credentials={"access_token": Secret.load("gh-repo-access-token")},
    )

    flow.from_source(
        source=my_private_github_repo,
        entrypoint="path/to/my_remote_flow_code_file.py:entrypoint function",
    ).serve(
        name="stock-data-from-private-gh-repo",
        cron="0 0 * * *",
    )
