import time
import requests
import pytest
from typing import Generator
import docker
from kurrentdbclient import KurrentDBClient, AsyncKurrentDBClient
import platform
@pytest.fixture(scope="session")
def kurrentdb_container() -> Generator[None, None, None]:

    """
    Fixture to start a KurrentDB Docker container for testing.

    This fixture has session scope, so the container will be started once
    at the beginning of the test session and stopped after all tests complete.

    """

    # Just to see if there is a running instance
    try:
        # Check if the KurrentDB HTTP port is accessible
        response = requests.get("http://localhost:2113/info", timeout=5)
        if response.status_code == 200:
            print("A KurrentDB is already running. Using the existing instance.")
            yield
    except Exception as e:
        print(f"No running KurrentDB Found...")


    print("Starting KurrentDB container")
    if platform.system() == "Windows":
        client = docker.DockerClient(base_url="tcp://localhost:2375")
    elif platform.system() == "Linux" or platform.system() == "Darwin":
        client = docker.DockerClient(base_url="unix://var/run/docker.sock")
    else:
        raise Exception("Unsupported platform")
        
    print(client.version())

    # Container name
    container_name = "kurrentdb-test"

    # KurrentDB Docker image
    image = "eventstore/eventstore:24.10.4-alpha-arm64v8"

    # Pull the image
    client.images.pull(image)

    # Environment variables for KurrentDB
    environment = {
        "EVENTSTORE_CLUSTER_SIZE": "1",
        "EVENTSTORE_RUN_PROJECTIONS": "All",
        "EVENTSTORE_START_STANDARD_PROJECTIONS": "true",
        "EVENTSTORE_INSECURE": "true",
        "EVENTSTORE_ENABLE_ATOM_PUB_OVER_HTTP": "true"
    }

    # Port mappings
    ports = {
        '1113/tcp': 1113,
        '2113/tcp': 2113
    }

    # Start container
    container = client.containers.run(
        image,
        name=container_name,
        detach=True,
        environment=environment,
        ports=ports,
        auto_remove=True,
        mem_swappiness=0
    )

    print(f"Started KurrentDB container: {container.id[:12]}")

    # Wait for KurrentDB to be ready
    max_retries = 30
    retry_interval = 5
    timeout_seconds = 5
    for i in range(max_retries):
        try:
            # Check if the container is running
            container.reload()
            if container.status != "running":
                raise Exception("Container stopped unexpectedly")

            # Check if the HTTP port is accessible
            response = requests.get("http://localhost:2113/info", timeout=timeout_seconds)
            if response.status_code == 200:
                print("KurrentDB is ready")
                break
        except Exception as e:
            print(f"Waiting for KurrentDB to be ready... ({i + 1}/{max_retries})")

        if i == max_retries - 1:
            # If we've reached the maximum number of retries, stop the container and fail
            container.stop()
            pytest.fail("KurrentDB failed to start")

        time.sleep(retry_interval)

    # KurrentDB is ready, yield control back to the tests
    yield
    
    # Stop and remove container after tests
    print("Stopping KurrentDB container")
    # input("Press Enter to continue...")
    container.stop()

@pytest.fixture(scope="session")
def client():
    return KurrentDBClient(uri="esdb://localhost:2113?Tls=false")

@pytest.fixture(scope="session")
def async_client():
    return AsyncKurrentDBClient(uri="esdb://localhost:2113?Tls=false")