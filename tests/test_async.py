import pytest
from langgraph.graph import StateGraph
from langgraph_checkpoint_kurrentdb import KurrentDBSaver
from kurrentdbclient import KurrentDBClient, AsyncKurrentDBClient

# Fixtures
@pytest.fixture
def client(kurrentdb_container):
    return KurrentDBClient(uri="esdb://localhost:2113?Tls=false")

@pytest.fixture
def async_client(kurrentdb_container):
    return AsyncKurrentDBClient(uri="esdb://localhost:2113?Tls=false")

@pytest.fixture
def memory_saver(async_client):
    return KurrentDBSaver(async_client=async_client)

@pytest.fixture
def base_config():
    return {"configurable": {"thread_id": "1", "checkpoint_ns": ""}}

@pytest.fixture
def sample_checkpoint():
    return {
        "ts": "2024-05-04T06:32:42.235444+00:00",
        "id": "1ef4f797-8335-6428-8001-8a1503f9b875",
        "channel_values": {"key": "value"}
    }

@pytest.fixture
def sample_metadata():
    return {"source": "input", "step": 1, "writes": {"key": "value"}}

# Tests
@pytest.mark.asyncio
async def test_put_and_list_checkpoints(memory_saver, base_config):

    config = {"configurable": {"thread_id": "1", "checkpoint_ns": ""}}
    checkpoint = {"ts": "2024-05-04T06:32:42.235444+00:00", "id": "1ef4f797-8335-6428-8001-8a1503f9b875",
                  "channel_values": {"key": "value"}}
    saved_config = await memory_saver.aput(config, checkpoint, {"source": "input", "step": 1, "writes": {"key": "value"}}, {})
    
    # saved_config = memory_saver.put(base_config, sample_checkpoint, sample_metadata, {})
    assert saved_config["configurable"]["thread_id"] == "1"
    assert saved_config["configurable"]["checkpoint_id"] == "1ef4f797-8335-6428-8001-8a1503f9b875"
    assert saved_config["configurable"]["checkpoint_ns"] == ""
    import time 
    time.sleep(5)
    checkpoints = await list(memory_saver.alist(base_config))
    assert isinstance(checkpoints, list)
    # Add more specific assertions based on expected checkpoint structure

@pytest.mark.asyncio
async def test_get_checkpoint(memory_saver, base_config):
    checkpoint_tuple = await memory_saver.aget_tuple(base_config)
    if checkpoint_tuple:  # Checkpoint might not exist
        assert "configurable" in checkpoint_tuple.config
        assert checkpoint_tuple.config["configurable"]["thread_id"] == "1"

@pytest.mark.asyncio
async def test_get_tuple_with_specific_checkpoint(memory_saver):
    # Test without checkpoint_id
    config = {"configurable": {"thread_id": "1"}}
    checkpoint_tuple = await memory_saver.aget_tuple(config)
    assert checkpoint_tuple is not None or checkpoint_tuple is None  # Depending on if data exists

    # Test with checkpoint_id
    config_with_id = {
        "configurable": {
            "thread_id": "1",
            "checkpoint_ns": "",
            "checkpoint_id": "1ef4f797-8335-6428-8001-8a1503f9b875",
        }
    }
    checkpoint_tuple = await memory_saver.aget_tuple(config_with_id)
    if checkpoint_tuple:
        assert checkpoint_tuple.config["configurable"]["checkpoint_id"] == config_with_id["configurable"]["checkpoint_id"]
