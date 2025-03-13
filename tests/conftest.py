import os
import time
import requests
import pytest
from typing import Generator
import docker


@pytest.fixture(scope="session")
def kurrentdb_container() -> Generator[None, None, None]:
    """
    Fixture to start a KurrentDB Docker container for testing.
    
    This fixture has session scope, so the container will be started once
    at the beginning of the test session and stopped after all tests complete.
    """
    # Docker client
    client = docker.from_env()
    
    # Container name
    container_name = "kurrentdb-test"
    
    # KurrentDB Docker image
    image = "eventstore/eventstore:24.10.4-alpha-arm64v8"
    
    # Clean up any existing container with the same name
    try:
        existing_container = client.containers.get(container_name)
        print(f"Found existing container {container_name}, removing it...")
        existing_container.remove(force=True)
        print(f"Removed existing container {container_name}")
    except docker.errors.NotFound:
        # No existing container, which is fine
        pass
    
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
        remove=True
    )
    
    print(f"Started KurrentDB container: {container.id[:12]}")
    
    # Wait for KurrentDB to be ready
    max_retries = 30
    retry_interval = 2
    
    for i in range(max_retries):
        try:
            # Check if the container is running
            container.reload()
            if container.status != "running":
                raise Exception("Container stopped unexpectedly")
            
            # Check if the HTTP port is accessible
            response = requests.get("http://localhost:2113/health/live", timeout=1)
            if response.status_code == 200:
                print("KurrentDB is ready")
                break
        except Exception as e:
            print(f"Waiting for KurrentDB to be ready... ({i+1}/{max_retries})")
        
        if i == max_retries - 1:
            # If we've reached the maximum number of retries, stop the container and fail
            container.stop()
            pytest.fail("KurrentDB failed to start")
        
        time.sleep(retry_interval)
    
    # KurrentDB is ready, yield control back to the tests
    yield
    
    # Stop and remove container after tests
    print("Stopping KurrentDB container")
    container.stop()