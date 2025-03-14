# LangGraph Checkpoint KurrentDB

KurrentDB checkpoint implementation for LangGraph.

## Installation

```bash
pip install langgraph-checkpoint-kurrentdb
```

Or using Poetry:

```bash
poetry add langgraph-checkpoint-kurrentdb
```

## Usage

Setting up the KurrentDB checkpointer for your LangGraph:

```python
from langgraph.graph import StateGraph
from langgraph_checkpoint_kurrentdb import KurrentDBSaver
from kurrentdbclient import KurrentDBClient

# Initialize the KurrentDB client
client = KurrentDBClient(uri="esdb://localhost:2113?Tls=false")

# Create the KurrentDB checkpoint saver
saver = KurrentDBSaver(client)

# Create your LangGraph
builder = StateGraph(int)
builder.add_node("add_one", lambda x: x + 1)
builder.set_entry_point("add_one")
builder.set_finish_point("add_one")

# Compile with the KurrentDB checkpointer
graph = builder.compile(checkpointer=saver)

# Use thread ID to identify this execution
config = {"configurable": {"thread_id": "example-thread-1", "checkpoint_ns": ""}}
result = graph.invoke(42, config)

# Later, retrieve the state
state = graph.get_state(config)

# Get the trace
df = saver.trace(config["configurable"]["thread_id"])
print(df)

```