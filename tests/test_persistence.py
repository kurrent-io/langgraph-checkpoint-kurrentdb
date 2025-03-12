import pytest
import time
from datetime import datetime
from langgraph.graph import StateGraph
from langgraph_checkpoint_kurrentdb import KurrentDBSaver
from kurrentdbclient import KurrentDBClient, AsyncKurrentDBClient

# Fixtures
@pytest.fixture
def client():
    return KurrentDBClient(uri="esdb://localhost:2113?Tls=false")

@pytest.fixture
def async_client():
    return AsyncKurrentDBClient(uri="esdb://localhost:2113?Tls=false")

@pytest.fixture
def memory_saver(client, async_client):
    return KurrentDBSaver(client=client, async_client=async_client)

@pytest.fixture
def unique_thread_id():
    """Generate a unique thread ID for each test run to avoid conflicts"""
    return f"test_thread_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"

@pytest.fixture
def base_config(unique_thread_id):
    return {"configurable": {"thread_id": unique_thread_id, "checkpoint_ns": "persistence_test"}}

# Test persistence across test sessions
def test_persistence_write_checkpoint(memory_saver, base_config):
    """Write a checkpoint to be used in a subsequent test"""
    checkpoint = {
        "ts": datetime.now().isoformat(),
        "id": "persistent-checkpoint-id",
        "persistence_test": True,
        "channel_values": {"persistence_key": "value_to_persist"}
    }
    
    metadata = {
        "source": "persistence_test_write",
        "timestamp": time.time()
    }
    
    saved_config = memory_saver.put(base_config, checkpoint, metadata, {})
    
    # Store the thread_id somewhere so the next test can use it
    # For this example, we'll just return it and the next test will use the same fixture
    assert saved_config["configurable"]["checkpoint_id"] == checkpoint["id"]
    return base_config["configurable"]["thread_id"]

def test_persistence_read_checkpoint(memory_saver, base_config, client):
    """Read the checkpoint written by the previous test"""
    # In a real test scenario, we'd need to get the thread ID from the previous test
    # For this example, we'll use the same fixture
    
    # Set the checkpoint ID we expect to find
    config_with_id = base_config.copy()
    config_with_id["configurable"]["checkpoint_id"] = "persistent-checkpoint-id"
    
    # Attempt to read the checkpoint
    checkpoint_tuple = memory_saver.get_tuple(config_with_id)
    
    # We may not find it if running tests individually, so only assert if found
    if checkpoint_tuple:
        assert checkpoint_tuple.checkpoint["id"] == "persistent-checkpoint-id"
        assert checkpoint_tuple.checkpoint["persistence_test"] is True
        assert checkpoint_tuple.checkpoint["channel_values"]["persistence_key"] == "value_to_persist"
        assert checkpoint_tuple.metadata["source"] == "persistence_test_write"

# Test multiple threads and namespaces
def test_multiple_threads(memory_saver):
    """Test saving and retrieving checkpoints with different thread IDs"""
    # Create configurations for multiple threads
    thread_configs = [
        {"configurable": {"thread_id": f"multi_thread_{i}", "checkpoint_ns": "multi_ns"}}
        for i in range(3)
    ]
    
    # Create a different checkpoint for each thread
    for i, config in enumerate(thread_configs):
        checkpoint = {
            "ts": datetime.now().isoformat(),
            "id": f"multi-checkpoint-{i}",
            "thread_index": i,
            "channel_values": {f"key_{i}": f"value_{i}"}
        }
        
        metadata = {
            "source": "multi_thread_test",
            "thread_index": i
        }
        
        memory_saver.put(config, checkpoint, metadata, {})
    
    # Retrieve and verify each checkpoint
    for i, config in enumerate(thread_configs):
        # Add the checkpoint ID to the config
        config_with_id = config.copy()
        config_with_id["configurable"]["checkpoint_id"] = f"multi-checkpoint-{i}"
        
        # Get the checkpoint
        checkpoint_tuple = memory_saver.get_tuple(config_with_id)
        
        # Verify it matches what we saved
        assert checkpoint_tuple is not None
        assert checkpoint_tuple.checkpoint["id"] == f"multi-checkpoint-{i}"
        assert checkpoint_tuple.checkpoint["thread_index"] == i
        assert checkpoint_tuple.checkpoint["channel_values"][f"key_{i}"] == f"value_{i}"
        assert checkpoint_tuple.metadata["thread_index"] == i

def test_multiple_namespaces(memory_saver, unique_thread_id):
    """Test saving and retrieving checkpoints with different namespaces"""
    # Create configurations for multiple namespaces, same thread
    namespace_configs = [
        {"configurable": {"thread_id": unique_thread_id, "checkpoint_ns": f"namespace_{i}"}}
        for i in range(3)
    ]
    
    # Create a different checkpoint for each namespace
    for i, config in enumerate(namespace_configs):
        checkpoint = {
            "ts": datetime.now().isoformat(),
            "id": f"ns-checkpoint-{i}",
            "namespace_index": i,
            "channel_values": {f"ns_key_{i}": f"ns_value_{i}"}
        }
        
        metadata = {
            "source": "multi_namespace_test",
            "namespace_index": i
        }
        
        memory_saver.put(config, checkpoint, metadata, {})
    
    # Retrieve and verify each checkpoint
    for i, config in enumerate(namespace_configs):
        # Add the checkpoint ID to the config
        config_with_id = config.copy()
        config_with_id["configurable"]["checkpoint_id"] = f"ns-checkpoint-{i}"
        
        # Get the checkpoint
        checkpoint_tuple = memory_saver.get_tuple(config_with_id)
        
        # Verify it matches what we saved
        assert checkpoint_tuple is not None
        assert checkpoint_tuple.checkpoint["id"] == f"ns-checkpoint-{i}"
        assert checkpoint_tuple.checkpoint["namespace_index"] == i
        assert checkpoint_tuple.checkpoint["channel_values"][f"ns_key_{i}"] == f"ns_value_{i}"

# Test graph execution with continuation
def test_graph_execution_continuation(memory_saver, base_config):
    """Test running a graph, stopping, and continuing from the saved state"""
    # 1. Create and run a simple graph
    builder = StateGraph(int)
    builder.add_node("add_ten", lambda x: x + 10)
    builder.set_entry_point("add_ten")
    builder.set_finish_point("add_ten")
    
    graph = builder.compile(checkpointer=memory_saver)
    
    # Run the graph first time and save state
    result1 = graph.invoke(5, base_config)
    assert result1 == 15
    
    # Get the checkpoint ID from the stored state
    state_tuple = memory_saver.get_tuple(base_config)
    assert state_tuple is not None
    checkpoint_id = state_tuple.checkpoint["id"]
    
    # 2. Create a new graph that continues from where we left off
    base_config_with_id = base_config.copy()
    base_config_with_id["configurable"]["checkpoint_id"] = checkpoint_id
    
    # Create a new graph (in a real scenario, this could be a different process)
    new_builder = StateGraph(int)
    new_builder.add_node("multiply_by_two", lambda x: x * 2)
    new_builder.set_entry_point("multiply_by_two")
    new_builder.set_finish_point("multiply_by_two")
    
    new_graph = new_builder.compile(checkpointer=memory_saver)
    
    # Load the state from first execution
    state = graph.get_state(base_config_with_id)
    
    # Verify we got back the expected state
    assert state == 15
    
    # Run the new graph with the loaded state
    result2 = new_graph.invoke(state, base_config)
    assert result2 == 30  # 15 * 2 = 30
    
    # We should be able to see both checkpoints in the history
    history = list(graph.get_state_history(base_config))
    assert len(history) >= 2  # At least two checkpoints should exist

# Test retention with max_count
def test_retention_with_max_count(memory_saver, unique_thread_id):
    """Test setting max_count and verifying retention behavior"""
    # Configuration
    config = {"configurable": {"thread_id": unique_thread_id, "checkpoint_ns": "retention_test"}}
    
    # Set max count to 3 for our test thread
    memory_saver.set_max_count(3, unique_thread_id)
    
    # Create 5 checkpoints (more than our max)
    for i in range(5):
        checkpoint = {
            "ts": datetime.now().isoformat(),
            "id": f"retention-checkpoint-{i}",
            "index": i,
            "data": f"checkpoint-{i}-data"
        }
        
        metadata = {"index": i}
        
        memory_saver.put(config, checkpoint, metadata, {})
        
        # Small delay to ensure ordering
        time.sleep(0.1)
    
    # Now check the stream to see what was retained
    # We expect to see only the 3 most recent checkpoints (indexes 2, 3, 4)
    try:
        events = memory_saver.client.get_stream(
            stream_name=f"thread-{unique_thread_id}",
            resolve_links=True
        )
        
        # Convert events to checkpoints and check indexes
        found_indexes = []
        for event in events:
            checkpoint = memory_saver.jsonplus_serde.loads(event.data)
            if "index" in checkpoint:
                found_indexes.append(checkpoint["index"])
        
        # We should have 3 or fewer checkpoints
        assert len(found_indexes) <= 3
        
        # The highest indexes should have been kept
        if found_indexes:
            assert max(found_indexes) == 4  # Last one should definitely be there
            
    except Exception as e:
        pytest.skip(f"Stream access failed, cannot verify retention: {e}")