from collections import defaultdict

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
async def test_put_and_list_checkpoints(async_client, base_config):
    await async_client.connect()
    memory_saver = KurrentDBSaver(async_client=async_client)
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
    checkpoints = memory_saver.alist(base_config)
    #loop through async_generator checkpoints
    checkpoints = [checkpoint async for checkpoint in checkpoints]
    assert len(checkpoints) > 0
    # Add more specific assertions based on expected checkpoint structure

@pytest.mark.asyncio
async def test_get_checkpoint(async_client, base_config):
    await async_client.connect()
    memory_saver = KurrentDBSaver(async_client=async_client)
    checkpoint_tuple = await memory_saver.aget_tuple(base_config)
    if checkpoint_tuple:  # Checkpoint might not exist
        assert "configurable" in checkpoint_tuple.config
        assert checkpoint_tuple.config["configurable"]["thread_id"] == "1"

@pytest.mark.asyncio
async def test_get_tuple_with_specific_checkpoint(async_client):
    # Test without checkpoint_id
    await async_client.connect()
    memory_saver = KurrentDBSaver(async_client=async_client)
    config = {"configurable": {"thread_id": "test_get_tuple_with_specific_checkpoint"}}
    checkpoint_tuple = await memory_saver.aget_tuple(config)
    assert checkpoint_tuple is None

    # Test with checkpoint_id
    config_with_id = {
        "configurable": {
            "thread_id": "test_get_tuple_with_specific_checkpoint",
            "checkpoint_ns": "",
            "checkpoint_id": "1ef4f797-8335-6428-8001-8a1503f9b875",
        }
    }
    checkpoint_tuple = await memory_saver.aget_tuple(config_with_id)
    if checkpoint_tuple:
        assert checkpoint_tuple.config["configurable"]["checkpoint_id"] == config_with_id["configurable"]["checkpoint_id"]

@pytest.mark.asyncio
def test_simple_graph_execution_async(async_client, base_config):
    async_client.connect()
    memory_saver = KurrentDBSaver(async_client=async_client)
    config = {"configurable": {"thread_id": "simple-graph-execution-test"}}
    # Build and run simple graph
    builder = StateGraph(int)
    builder.add_node("add_one", lambda x: x + 1)
    builder.set_entry_point("add_one")
    builder.set_finish_point("add_one")

    graph = builder.compile(checkpointer=memory_saver)

    # Test initial state
    initial_state = graph.get_state(config)
    assert initial_state is not None

    # Test execution
    result = graph.ainvoke(3, config).__await__()
    # Test final state
    final_state = graph.get_state(config)
    assert final_state is not None

    # Access the state values directly (no need to call it)
    assert final_state[0] == 4  # Check final value after add_one


def test_subgraph_execution(async_client, base_config):
    async_client.connect()
    memory_saver = KurrentDBSaver(async_client=async_client)

    memory_saver.writes = defaultdict(dict)
    config = {"configurable": {"thread_id": "main-graph"}}
    # Main graph
    builder = StateGraph(int)
    builder.add_node("add_one", lambda x: x + 1)
    builder.set_entry_point("add_one")

    # Subgraph
    subgraph_builder = StateGraph(int)
    subgraph_builder.add_node("add_two", lambda x: x + 2)
    subgraph_builder.set_entry_point("add_two")
    subgraph_builder.set_finish_point("add_two")

    subgraph = subgraph_builder.compile(checkpointer=memory_saver)
    builder.add_node("subgraph", subgraph)
    builder.add_edge("add_one", "subgraph")
    builder.set_finish_point("subgraph")

    graph = builder.compile(checkpointer=memory_saver)
    # Test execution
    result = graph.ainvoke(3, config).__await__()
    assert result == 6  # 3 + 1 + 2 = 6

    # Test state history
    state_history = list(graph.get_state_history(config))
    assert len(state_history) > 0