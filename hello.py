from prefect import flow


@flow
def hello():
    print("Hello, world!")


if __name__ == "__main__":
    hello()
