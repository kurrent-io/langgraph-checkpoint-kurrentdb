import pytest
import asyncio
from unittest.mock import MagicMock, patch
from langgraph.graph import StateGraph
from langgraph_checkpoint_kurrentdb import KurrentDBSaver
from kurrentdbclient import KurrentDBClient, AsyncKurrentDBClient, exceptions

# Fixtures
@pytest.fixture
def client(kurrentdb_container):
    return KurrentDBClient(uri="esdb://localhost:2113?Tls=false")

@pytest.fixture
def async_client(kurrentdb_container):
    client = AsyncKurrentDBClient(uri="esdb://localhost:2113?Tls=false")
    client.connect().__await__();
    return client

@pytest.fixture
def memory_saver(client, async_client):
    return KurrentDBSaver(client=client, async_client=async_client)

@pytest.fixture
def base_config():
    return {"configurable": {"thread_id": "test_advanced", "checkpoint_ns": "advanced_ns"}}

@pytest.fixture
def sample_checkpoint():
    return {
        "ts": "2024-05-04T06:32:42.235444+00:00",
        "id": "advanced-checkpoint-id",
        "channel_values": {"test_key": "test_value"},
        "channel_versions": ["start:node1", "node1", "node2"]
    }

@pytest.fixture
def sample_metadata():
    return {"source": "advanced_test", "step": 1, "writes": {"key": "value"}}

# Test put_writes method
def test_put_writes(memory_saver, base_config):
    # Prepare test data
    writes = [
        ("channel1", "value1"),
        ("channel2", {"complex": "value", "nested": {"data": 33}}),
        ("channel3", [1, 2, 3, 4, 5])
    ]
    task_id = "test_task_id"
    task_path = "test/path"
    
    # Execute the method
    memory_saver.put_writes(base_config, writes, task_id, task_path)
    
    # Check that writes are stored in memory
    thread_id = base_config["configurable"]["thread_id"]
    checkpoint_ns = base_config["configurable"]["checkpoint_ns"]
    checkpoint_id = base_config["configurable"]["checkpoint_id"]
    
    assert (thread_id, checkpoint_ns, checkpoint_id) in memory_saver.writes
    
    # Check that the writes have been stored in the expected format
    for idx, (channel, _) in enumerate(writes):
        inner_key = (task_id, idx)
        assert inner_key in memory_saver.writes[(thread_id, checkpoint_ns, checkpoint_id)]
        
        stored_tuple = memory_saver.writes[(thread_id, checkpoint_ns, checkpoint_id)][inner_key]
        assert stored_tuple[0] == task_id
        assert stored_tuple[1] == channel
        assert stored_tuple[3] == task_path

# Test get_next_version
def test_get_next_version(memory_saver):
    # Mock channel protocol
    channel = MagicMock()
    
    # Test with None as current version
    next_version = memory_saver.get_next_version(None, channel)
    assert next_version.startswith("1")
    
    # Test with integer as current version
    next_version = memory_saver.get_next_version(1, channel)
    assert next_version.startswith("2")
    
    # Test with string version (format: "version.hash")
    next_version = memory_saver.get_next_version("5.123456", channel)
    assert next_version.startswith("6")
    
    # Check that the versions are monotonically increasing
    versions = []
    for _ in range(10):
        versions.append(memory_saver.get_next_version(None, channel))
    
    # Convert the version part to int for comparison
    version_numbers = [int(v.split(".")[0]) for v in versions]
    for i in range(1, len(version_numbers)):
        assert version_numbers[i] > version_numbers[i-1]

# Test error handling
def test_error_handling_stream_not_found(memory_saver):
    # Create a config with a non-existent thread ID
    config = {"configurable": {"thread_id": "non_existent_thread", "checkpoint_ns": ""}}
    
    # get_tuple should return None for a non-existent stream
    assert memory_saver.get_tuple(config) is None

@patch.object(KurrentDBClient, 'get_stream')
def test_error_handling_client_exception(mock_get_stream, memory_saver, base_config):
    # Mock get_stream to raise an exception
    mock_get_stream.side_effect = Exception("Test exception")
    
    # Test get_tuple with the mocked client - should raise the exception
    with pytest.raises(Exception) as excinfo:
        memory_saver.get_tuple(base_config)
    assert "Test exception" in str(excinfo.value)

# Test set_max_count
def test_set_max_count(memory_saver):
    thread_id = "test_max_count_thread"
    max_count = 100
    
    with patch.object(memory_saver.client, 'set_stream_metadata') as mock_set_metadata:
        memory_saver.set_max_count(max_count, thread_id)
        
        # Check that set_stream_metadata was called with the right arguments
        mock_set_metadata.assert_called_once()
        call_args = mock_set_metadata.call_args[1]
        assert call_args["stream_name"] == f"thread-{thread_id}"
        assert call_args["metadata"] == {"$maxCount": max_count}

# Test complex state objects
def test_complex_state_objects(memory_saver, base_config):
    # Create a complex nested state object
    complex_checkpoint = {
        "ts": "2024-05-04T06:32:42.235444+00:00",
        "id": "complex-state-id",
        "channel_values": {
            "simple_key": "simple_value",
            "nested_dict": {
                "level1": {
                    "level2": {
                        "level3": "deep_value"
                    },
                    "array": [1, 2, 3, {"key": "value"}]
                }
            },
            "mixed_types": [1, "string", True, None, {"key": [4, 5, 6]}]
        }
    }
    
    complex_metadata = {
        "source": "complex_test",
        "nested_metadata": {
            "custom": {
                "metrics": [0.1, 0.2, 0.3],
                "flags": {"feature1": True, "feature2": False}
            }
        }
    }
    
    # Save the complex state
    saved_config = memory_saver.put(base_config, complex_checkpoint, complex_metadata, {})
    
    # Retrieve it back
    config_with_id = {
        "configurable": {
            "thread_id": base_config["configurable"]["thread_id"],
            "checkpoint_ns": base_config["configurable"]["checkpoint_ns"],
            "checkpoint_id": complex_checkpoint["id"]
        }
    }
    
    checkpoint_tuple = memory_saver.get_tuple(config_with_id)
    
    # Verify the complex state was saved and retrieved correctly
    assert checkpoint_tuple is not None
    assert checkpoint_tuple.checkpoint["id"] == complex_checkpoint["id"]
    assert checkpoint_tuple.checkpoint["channel_values"]["nested_dict"]["level1"]["level2"]["level3"] == "deep_value"
    assert checkpoint_tuple.checkpoint["channel_values"]["mixed_types"][4]["key"] == [4, 5, 6]
    assert checkpoint_tuple.metadata["nested_metadata"]["custom"]["metrics"] == [0.1, 0.2, 0.3]

# Async tests
@pytest.mark.asyncio
async def test_aget_tuple(memory_saver, base_config, sample_checkpoint, sample_metadata):
    # First put a checkpoint using sync client
    memory_saver.put(base_config, sample_checkpoint, sample_metadata, {})
    
    # Then retrieve it using async client
    checkpoint_tuple = await memory_saver.aget_tuple(base_config)
    
    assert checkpoint_tuple is not None
    assert checkpoint_tuple.checkpoint["id"] == sample_checkpoint["id"]
    assert checkpoint_tuple.metadata["source"] == sample_metadata["source"]

@pytest.mark.asyncio
async def test_aput(memory_saver, base_config, sample_checkpoint, sample_metadata):
    # Test the async put method
    saved_config = await memory_saver.aput(base_config, sample_checkpoint, sample_metadata, {})
    
    assert saved_config["configurable"]["thread_id"] == base_config["configurable"]["thread_id"]
    assert saved_config["configurable"]["checkpoint_id"] == sample_checkpoint["id"]
    
    # Verify it was saved by retrieving it
    checkpoint_tuple = await memory_saver.aget_tuple(saved_config)
    
    assert checkpoint_tuple is not None
    assert checkpoint_tuple.checkpoint["id"] == sample_checkpoint["id"]
    assert checkpoint_tuple.checkpoint["channel_values"]["test_key"] == "test_value"

@pytest.mark.asyncio
async def test_alist(memory_saver, base_config, sample_checkpoint, sample_metadata):
    # Put a checkpoint first
    await memory_saver.aput(base_config, sample_checkpoint, sample_metadata, {})
    
    # List checkpoints
    checkpoints = []
    async for checkpoint in await memory_saver.alist(base_config):
        checkpoints.append(checkpoint)
    
    # There should be at least one checkpoint
    assert len(checkpoints) > 0
    
    # Find our checkpoint in the list
    found = False
    for checkpoint in checkpoints:
        if checkpoint.checkpoint["id"] == sample_checkpoint["id"]:
            found = True
            break
    
    assert found, "Our checkpoint wasn't found in the list results"

# Test async graph execution
@pytest.mark.asyncio
async def test_async_graph_execution(memory_saver, base_config):
    # Build graph
    builder = StateGraph(int)
    
    # Add an async node function
    async def add_two_async(x):
        await asyncio.sleep(0.1)  # Simulate async work
        return x + 2
    
    builder.add_node("add_two", add_two_async)
    builder.set_entry_point("add_two")
    builder.set_finish_point("add_two")
    
    graph = builder.compile(checkpointer=memory_saver)
    
    # Execute graph asynchronously
    result = await graph.ainvoke(5, base_config)
    assert result == 7
    
    # Check state was saved
    config_with_id = base_config.copy()
    checkpoint_tuple = await memory_saver.aget_tuple(config_with_id)
    assert checkpoint_tuple is not None

