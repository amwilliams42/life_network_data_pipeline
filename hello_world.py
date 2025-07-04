from prefect import flow

@flow(log_prints=True)
def hello_world(name: str = ""):
    print(f"Hello, {name}!")

if __name__ == "__main__":
    hello_world("World")