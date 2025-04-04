import kurrentdbclient
from langgraph.checkpoint.base import EmptyChannelError
import threading
from typing import Any, AsyncIterator, Dict, Iterator, Optional, Sequence, Tuple
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer
from langgraph.checkpoint.serde.types import ChannelProtocol, TASKS
from langgraph.checkpoint.base import (
    WRITES_IDX_MAP,
    BaseCheckpointSaver,
    ChannelVersions,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
    SerializerProtocol,
    get_checkpoint_id
)
import random

from kurrentdbclient import KurrentDBClient, AsyncKurrentDBClient, NewEvent, StreamState, exceptions
from collections import defaultdict

class KurrentDBSaver(BaseCheckpointSaver[str]):
    """A KurrentDB-based checkpoint saver.
    Requirements:
    - by_category system projections enabled
    - optional: $ce-thread stream should be empty ideally because thread-checkpoint_id streams are used to
    keep checkpoints of each thread
    """
    client: KurrentDBClient
    async_client: AsyncKurrentDBClient

    writes: defaultdict[ #for in memory pending writes
        tuple[str, str, str],
        dict[tuple[str, int], tuple[str, str, tuple[str, bytes], str]],
    ]
    def __init__(
        self,
        client: KurrentDBClient = None,
        async_client: AsyncKurrentDBClient = None,
        *,
        serde: Optional[SerializerProtocol] = None,
        factory: type[defaultdict] = defaultdict,
    ) -> None:
        super().__init__(serde=serde)
        self.jsonplus_serde = JsonPlusSerializer()
        if client is None and async_client is None:
            raise Exception("At least one of sync or async client must be provided.")
        self.client = client
        self.async_client = async_client
        self.lock = threading.Lock()
        self.writes = factory(dict)


    def get_tuple(self, config: RunnableConfig) -> Optional[CheckpointTuple]:
        if self.client is None:
            raise Exception("Synchronous Client is required.")
        checkpoint_id = get_checkpoint_id(config)
        thread_id = config["configurable"]["thread_id"]
        try:
            checkpoints_events = self.client.get_stream(
                stream_name="thread-" + str(thread_id),
                resolve_links=True,
                backwards=True
            )
        except exceptions.NotFound as e:
            return None #no checkpoint found
        for event in checkpoints_events:
            checkpoint = self.jsonplus_serde.loads(event.data)
            keys = []
            versions = []
            if ("channel_values" in checkpoint and "keys" in checkpoint["channel_values"]
                    and "channel_versions" in checkpoint):
                keys = checkpoint["channel_values"].pop("keys")
                versions = checkpoint["channel_versions_kdb"]

            channel_values = {}
            metadata = self.jsonplus_serde.loads(event.metadata)
            parent_checkpoint_id = ""
            checkpoint_ns = ""
            if "checkpoint_ns" in metadata:
                checkpoint_ns = metadata["checkpoint_ns"]
            for key in keys:
                channel_values[self.breakdown_channel_stream_name(key)] = (
                    self.get_channel_value(key, versions, thread_id, checkpoint_ns, checkpoint["id"]))
            checkpoint["channel_values"] = channel_values

            if "parents" in metadata:
                if checkpoint_ns in metadata["parents"]:
                    parent_checkpoint_id = metadata["parents"][checkpoint_ns] 
                    # https://github.com/langchain-ai/langgraph/blob/e757a800019f6f943a78856cbe64fe1e3be4d32d/libs/checkpoint/langgraph/checkpoint/base/__init__.py#L38
            if parent_checkpoint_id != "" and parent_checkpoint_id is not None:
                sends = sorted(
                    (
                        (*w, k[1])
                        for k, w in self.writes[
                            (thread_id, checkpoint_ns, parent_checkpoint_id)
                        ].items()
                        if w[1] == TASKS
                    ),
                    key=lambda w: (w[3], w[0], w[4]),
                )
            else:
                sends = []
            checkpoint["pending_sends"] =  [self.serde.loads_typed(s[2]) for s in sends]
            writes = self.writes[(thread_id, parent_checkpoint_id, checkpoint['id'])].values()
            if checkpoint_id is None or checkpoint["id"] == checkpoint_id: #just return latest checkpoint
                return CheckpointTuple(
                {
                    "configurable": {
                        "thread_id": thread_id,
                        "checkpoint_ns": checkpoint_ns,
                        "checkpoint_id": checkpoint["id"],
                    }
                },
                checkpoint=checkpoint,
                metadata=metadata,
                parent_config=parent_checkpoint_id,
                pending_writes=[
                        (id, c, self.serde.loads_typed(v)) for id, c, v, _ in writes
                    ],
            )
        return None

    def list(
        self,
        config: Optional[RunnableConfig],
        *,
        filter: Optional[Dict[str, Any]] = None,
        before: Optional[RunnableConfig] = None,
        limit: Optional[int] = None,
    ) -> Iterator[CheckpointTuple]:
        if self.client is None:
            raise Exception("Synchronous Client is required.")

        if before is not None:
            raise NotImplementedError("Before is not supported yet")

        streams_events = self.client.get_stream(
            stream_name="$ce-thread",
            resolve_links=True
        )
        for event in streams_events:
            thread_id = event.stream_name.split("-")[1]
            checkpoint =  self.jsonplus_serde.loads(event.data)
            metadata = self.jsonplus_serde.loads(event.metadata)
            keys = []
            versions = []
            if ("channel_values" in checkpoint and "keys" in checkpoint["channel_values"]
                    and "channel_versions" in checkpoint):
                keys = checkpoint["channel_values"].pop("keys")
                versions = checkpoint["channel_versions_kdb"]

            channel_values = {}
            for key in keys:
                channel_values[self.breakdown_channel_stream_name(key)] = (
                    self.get_channel_value(key, versions, thread_id, checkpoint["id"], checkpoint["id"]))
            checkpoint["channel_values"] = channel_values

            parent_checkpoint_id = None
            checkpoint_ns = ""
            if "checkpoint_ns" in metadata:
                checkpoint_ns = metadata["checkpoint_ns"]
            if "parents" in metadata:
                if checkpoint_ns in metadata["parents"]:
                    parent_checkpoint_id = metadata["parents"][checkpoint_ns] 

            if parent_checkpoint_id != "" and parent_checkpoint_id is not None:
                sends = sorted(
                    (
                        (*w, k[1])
                        for k, w in self.writes[
                            (thread_id, checkpoint_ns, parent_checkpoint_id)
                        ].items()
                        if w[1] == TASKS
                    ),
                    key=lambda w: (w[3], w[0], w[4]),
                )
            else:
                sends = []
            checkpoint["pending_sends"] =  [self.serde.loads_typed(s[2]) for s in sends]

            if filter and not all(
                    query_value == metadata.get(query_key)
                    for query_key, query_value in filter.items()
            ):
                continue

            if checkpoint_ns is not None and checkpoint_ns != "":
                if config is not None and "configurable" in config and "checkpoint_ns" in config["configurable"]:
                    if checkpoint_ns != config["configurable"]["checkpoint_ns"]:
                        continue


            # limit search results
            if limit is not None and limit <= 0:
                break
            elif limit is not None:
                limit -= 1
            writes = self.writes[
                        (thread_id, checkpoint_ns, checkpoint['id'])
                    ].values()

            yield CheckpointTuple(
                {
                    "configurable": {
                        "thread_id": thread_id,
                        "checkpoint_ns": checkpoint_ns,
                        "checkpoint_id": checkpoint['id'],
                    }
                },
                checkpoint,
                metadata,
                parent_config=parent_checkpoint_id, 
                pending_writes=[
                    (id, c, self.serde.loads_typed(v)) for id, c, v, _ in writes
                ],
            )


    def put(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        """
        Store a checkpoint with its configuration and metadata.
        """
        if self.client is None:
            raise Exception("Synchronous Client is required.")
        c = checkpoint.copy()
        try:
            c.pop("pending_sends")  # type: ignore[misc]    
        except:
            c["pending_sends"] = []

        thread_id = config["configurable"]["thread_id"]
        checkpoint_ns = config["configurable"]["checkpoint_ns"]

        """
        Breakdown channel values
        Keep a pointer to streams which have the channel values in keys
        Rename Channel Versions to stream names
        """
        if "channel_values" in c:
            channel_keys = []
            if "channel_versions" not in c:
                c["channel_versions"] = {}
            for key in c["channel_values"]:
                channel_keys.append(self.build_channel_stream_name(thread_id, key, checkpoint_ns))
                if key not in c["channel_versions"]:
                    c["channel_versions"][key] = 0

            new_channel_version, new_versions_seen = self.breakdown_channel_values(thread_id, c["channel_values"], c["channel_versions"], checkpoint_ns)
            c["channel_values"] = {}  # empty dict
            c["channel_values"]["keys"] = channel_keys
            c["versions_seen_kdb"] = new_versions_seen
            c["channel_versions_kdb"] = new_channel_version

        serialized_checkpoint = self.jsonplus_serde.dumps(c)
        serialized_metadata = self.jsonplus_serde.dumps(metadata)

        checkpoint_event = NewEvent(
            type="langgraph_checkpoint",
            data=serialized_checkpoint,
            metadata=serialized_metadata,
            content_type='application/json',
        )
        self.client.append_to_stream(
            stream_name=f"thread-{thread_id}",
            events=[checkpoint_event],
            current_version=StreamState.ANY #Multiple state conflict resolution happens in Python reducers
        )

        return {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint["id"],
            }
        }

    def put_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[Tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        """
        Pending write are done in memory
        """
        thread_id = config["configurable"]["thread_id"]
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")
        checkpoint_id = config["configurable"]["checkpoint_id"]
        outer_key = (thread_id, checkpoint_ns, checkpoint_id)
        outer_writes_ = self.writes.get(outer_key)

        for idx, (c, v) in enumerate(writes):

            inner_key = (task_id, WRITES_IDX_MAP.get(c, idx))
            if inner_key[1] >= 0 and outer_writes_ and inner_key in outer_writes_:
                continue
            if len(c) > 0 and isinstance(c, str):
                c = self.build_channel_stream_name(thread_id, c, checkpoint_ns)
            self.writes[outer_key][inner_key] = (
                task_id,
                c,
                self.serde.dumps_typed(v),
                task_path,
            )

    async def aget_tuple(self, config: RunnableConfig) -> Optional[CheckpointTuple]:
        print("AGET TUPLE")
        if self.async_client is None:
            raise Exception("ASynchronous Client is required.")
        result: Optional[CheckpointTuple] = None
        checkpoint_id = get_checkpoint_id(config)
        thread_id = config["configurable"]["thread_id"]
        try:
            checkpoints_events = self.async_client.read_stream(
                stream_name="thread-" + str(thread_id),
                resolve_links=True,
                backwards=True
            )
            async for event in await checkpoints_events:
                checkpoint = self.jsonplus_serde.loads(event.data)
                keys = []
                versions = []
                if ("channel_values" in checkpoint and "keys" in checkpoint["channel_values"]
                        and "channel_versions" in checkpoint):
                    keys = checkpoint["channel_values"].pop("keys")
                    versions = checkpoint["channel_versions_kdb"]

                channel_values = {}
                metadata = self.jsonplus_serde.loads(event.metadata)
                parent_checkpoint_id = ""
                checkpoint_ns = ""
                if "checkpoint_ns" in metadata:
                    checkpoint_ns = metadata["checkpoint_ns"]
                for key in keys:
                    channel_values[self.breakdown_channel_stream_name(key)] = (
                        self.get_channel_value_async(key, versions, thread_id, checkpoint_ns, checkpoint["id"]))
                checkpoint["channel_values"] = channel_values

                if "parents" in metadata:
                    if checkpoint_ns in metadata["parents"]:
                        parent_checkpoint_id = metadata["parents"][checkpoint_ns] 
                        # https://github.com/langchain-ai/langgraph/blob/e757a800019f6f943a78856cbe64fe1e3be4d32d/libs/checkpoint/langgraph/checkpoint/base/__init__.py#L38
                if parent_checkpoint_id != "" and parent_checkpoint_id is not None:
                    sends = sorted(
                        (
                            (*w, k[1])
                            for k, w in self.writes[
                                (thread_id, checkpoint_ns, parent_checkpoint_id)
                            ].items()
                            if w[1] == TASKS
                        ),
                        key=lambda w: (w[3], w[0], w[4]),
                    )
                else:
                    sends = []
                checkpoint["pending_sends"] =  [self.serde.loads_typed(s[2]) for s in sends]
                writes = self.writes[(thread_id, parent_checkpoint_id, checkpoint['id'])].values()
                if checkpoint_id is None or checkpoint["id"] == checkpoint_id:
                    result = CheckpointTuple(
                                {
                                    "configurable": {
                                        "thread_id": thread_id,
                                        "checkpoint_ns": checkpoint_ns,
                                        "checkpoint_id": checkpoint["id"],
                                    }
                                },
                                checkpoint=checkpoint,
                                metadata=metadata,
                                parent_config=parent_checkpoint_id,
                                pending_writes=[
                                    (id, c, self.serde.loads_typed(v)) for id, c, v, _ in writes
                                ],
                            )
                    break
        except exceptions.NotFound:
            pass

        return result

    async def alist(
        self,
        config: Optional[RunnableConfig],
        *,
        filter: Optional[Dict[str, Any]] = None,
        before: Optional[RunnableConfig] = None,
        limit: Optional[int] = None,
    ) -> AsyncIterator[CheckpointTuple]:
        if self.async_client is None:
            raise Exception("ASynchronous Client is required.")

        if before is not None:
            raise NotImplementedError("Before is not supported yet")

        # Read thread category stream $ce-thread
        # this will give us all thread streams then we can read those to find the checkpoints
        streams_events = self.async_client.read_stream(
            stream_name="$ce-thread",
            resolve_links=True
        )
        async for event in await streams_events:
            thread_id = event.stream_name.split("-")[1]
            checkpoint = self.jsonplus_serde.loads(event.data)
            metadata = self.jsonplus_serde.loads(event.metadata)
            keys = []
            versions = []
            if ("channel_values" in checkpoint and "keys" in checkpoint["channel_values"]
                    and "channel_versions" in checkpoint):
                keys = checkpoint["channel_values"].pop("keys")
                versions = checkpoint["channel_versions_kdb"]

            channel_values = {}
            for key in keys:
                channel_values[self.breakdown_channel_stream_name(key)] = (
                    self.get_channel_value_async(key, versions, thread_id, checkpoint["id"], checkpoint["id"]))
            checkpoint["channel_values"] = channel_values
            parent_checkpoint_id = None
            checkpoint_ns = ""
            if "checkpoint_ns" in metadata:
                checkpoint_ns = metadata["checkpoint_ns"]
            if "parents" in metadata:
                if checkpoint_ns in metadata["parents"]:
                    parent_checkpoint_id = metadata["parents"][checkpoint_ns]

            sends = []

            if parent_checkpoint_id != "" and parent_checkpoint_id is not None:
                sends = sorted(
                    (
                        (*w, k[1])
                        for k, w in self.writes[
                            (thread_id, checkpoint_ns, parent_checkpoint_id)
                        ].items()
                        if w[1] == TASKS
                    ),
                    key=lambda w: (w[3], w[0], w[4]),
                )
            else:
                sends = []
            checkpoint["pending_sends"] =  [self.serde.loads_typed(s[2]) for s in sends]

            if filter and not all(
                    query_value == metadata.get(query_key)
                    for query_key, query_value in filter.items()
            ):
                continue

            if checkpoint_ns is not None and checkpoint_ns != "":
                if config is not None and "configurable" in config and "checkpoint_ns" in config["configurable"]:
                    if checkpoint_ns != config["configurable"]["checkpoint_ns"]:
                        continue


            # limit search results
            if limit is not None and limit <= 0:
                break
            elif limit is not None:
                limit -= 1
            writes = self.writes[
                        (thread_id, checkpoint_ns, checkpoint['id'])
                    ].values()

            yield CheckpointTuple(
                {
                    "configurable": {
                        "thread_id": thread_id,
                        "checkpoint_ns": checkpoint_ns,
                        "checkpoint_id": checkpoint['id'],
                    }
                },
                checkpoint,
                metadata,
                parent_config=parent_checkpoint_id,
                pending_writes=[
                    (id, c, self.serde.loads_typed(v)) for id, c, v, _ in writes
                ],
            )

    async def aput(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        print("APUT")
        """
        Store a checkpoint with its configuration and metadata.
        """
        if self.async_client is None:
            raise Exception("ASynchronous Client is required.")
        try:
            c = checkpoint.copy()
            c.pop("pending_sends")  # type: ignore[misc]    
        except:
            c["pending_sends"] = []

        thread_id = config["configurable"]["thread_id"]
        checkpoint_ns = config["configurable"]["checkpoint_ns"]

        """
        Breakdown channel values
        Keep a pointer to streams which have the channel values in keys
        Rename Channel Versions to stream names
        """
        if "channel_values" in c:
            channel_keys = []
            if "channel_versions" not in c:
                c["channel_versions"] = {}
            for key in c["channel_values"]:
                channel_keys.append(self.build_channel_stream_name(thread_id, key, checkpoint_ns))
                if key not in c["channel_versions"]:
                    c["channel_versions"][key] = 0

            new_channel_version, new_versions_seen = self.breakdown_channel_values_async(thread_id, c["channel_values"],
                                                                                   c["channel_versions"], checkpoint_ns)
            c["channel_values"] = {}  # empty dict
            c["channel_values"]["keys"] = channel_keys
            c["versions_seen_kdb"] = new_versions_seen
            c["channel_versions_kdb"] = new_channel_version

        serialized_checkpoint = self.jsonplus_serde.dumps(c)
        serialized_metadata = self.jsonplus_serde.dumps(metadata)

        checkpoint_event = NewEvent(
            type="langgraph_checkpoint",
            data=serialized_checkpoint,
            metadata=serialized_metadata,
            content_type='application/json',
        )

        await self.async_client.append_to_stream(
            stream_name=f"thread-{thread_id}",
            events=[checkpoint_event],
            current_version=StreamState.ANY
        )

        return {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint["id"],
            }
        }

    @staticmethod
    def build_channel_stream_name(thread_id: str, key: str, checkpoint_ns: str):
        stream_name = thread_id
        if checkpoint_ns is not None:
            stream_name = str(stream_name) +'_'+ str(checkpoint_ns)
        return stream_name + "->" + key

    @staticmethod
    def breakdown_channel_stream_name(stream_name: str):
        return stream_name.split("->")[1]

    def breakdown_channel_values(self, thread_id,
                                 channel_values: dict[str, Any],
                                 channel_versions: dict[str, str],
                                 checkpoint_ns: str = None):
        new_channel_version = {}
        new_channel_seen = {}
        if self.client is not None:
            # executing synchronously
            for key in channel_values:
                serialized_value = self.jsonplus_serde.dumps(channel_values[key])
                stream_name = self.build_channel_stream_name(thread_id, key, checkpoint_ns)
                metadata = self.jsonplus_serde.dumps({"langgraph_version":channel_versions[key]})
                checkpoint_event = NewEvent(
                    type="channel_value",
                    data=serialized_value,
                    content_type='application/json',
                    metadata=metadata
                )
                next_version = 0
                new_channel_seen[stream_name] = -1
                try:
                    events = self.client.get_stream(stream_name=f"{stream_name}", resolve_links=False, backwards=True, limit=2)
                except kurrentdbclient.exceptions.NotFound as e:
                    pass
                else:
                    if len(events) == 2:
                        new_channel_seen[stream_name] = events[0].stream_position
                        next_version = events[1].stream_position + 1
                    if len(events) == 1:
                        new_channel_seen[stream_name] = 0
                        next_version = events[0].stream_position + 1
                    for event in events:
                        next_version = event.stream_position + 1
                        break
                self.client.append_to_stream(
                    stream_name=f"{stream_name}",
                    events=[checkpoint_event],
                    current_version=StreamState.ANY #conflict resolution done on Python side
                )
                new_channel_version[stream_name] = next_version
            return new_channel_version, new_channel_seen

    async def breakdown_channel_values_async(self, thread_id,
                                 channel_values: dict[str, Any],
                                 channel_versions: dict[str, str],
                                 checkpoint_ns: str = None):
        new_channel_version = {}
        new_channel_seen = {}
        if self.async_client is not None:
            # executing asynchronously
            for key in channel_values:
                serialized_value = self.jsonplus_serde.dumps(channel_values[key])
                stream_name = self.build_channel_stream_name(thread_id, key, checkpoint_ns)
                metadata = self.jsonplus_serde.dumps({"langgraph_version": channel_versions[key]})
                checkpoint_event = NewEvent(
                    type="channel_value",
                    data=serialized_value,
                    content_type='application/json',
                    metadata=metadata
                )
                next_version = 0
                new_channel_seen[stream_name] = -1
                try:
                    reads = await self.async_client.get_stream(stream_name=f"{stream_name}", resolve_links=False, backwards=True,
                                                    limit=2)
                except kurrentdbclient.exceptions.NotFound as e:
                    pass
                else:
                    events = reads
                    # for event in reads:
                    #     events.append(event)
                    if len(events) == 2:
                        new_channel_seen[stream_name] = events[0].stream_position
                        next_version = events[1].stream_position + 1
                    if len(events) == 1:
                        new_channel_seen[stream_name] = 0
                        next_version = events[0].stream_position + 1
                    for event in events:
                        next_version = event.stream_position + 1
                        break
                self.async_client.append_to_stream(
                    stream_name=f"{stream_name}",
                    events=[checkpoint_event],
                    current_version=StreamState.ANY  # conflict resolution done on Python side
                ).__await__()
                new_channel_version[stream_name] = next_version
            print(new_channel_version, new_channel_seen)
            return new_channel_version, new_channel_seen


    def get_channel_value(self, channel_name: str, channel_versions, thread_id: str, checkpoint_ns: str, checkpoint_id: str):
        if self.client is not None:
            expected_position = 0
            if channel_name in channel_versions:
                if isinstance(channel_versions[channel_name], int):
                    expected_position = channel_versions[channel_name]
            stream_name = channel_name

            #get latest write from pending writes if present
            if (thread_id, checkpoint_ns, checkpoint_id) in self.writes:
                for key, value in self.writes[(thread_id, checkpoint_ns, checkpoint_id)].items().__reversed__():
                    if key[1] == channel_name:
                        return value[2]

            """
            Read only 1 event from the stream based on the channel version/stream position
            """
            try:
                events = self.client.read_stream(stream_name,
                                                 resolve_links=True,
                                                 backwards=True,
                                                 stream_position=expected_position,
                                                 limit=1)
                for event in events:
                    return self.jsonplus_serde.loads(event.data)
            except kurrentdbclient.exceptions.NotFound as e:
                return None
        return None

    def get_channel_value_async(self, channel_name: str, channel_versions, thread_id: str, checkpoint_ns: str, checkpoint_id: str):
        if self.async_client is not None:
            expected_position = 0
            if channel_name in channel_versions:
                if isinstance(channel_versions[channel_name], int):
                    expected_position = channel_versions[channel_name]
            stream_name = channel_name

            #get latest write from pending writes if present
            if (thread_id, checkpoint_ns, checkpoint_id) in self.writes:
                for key, value in self.writes[(thread_id, checkpoint_ns, checkpoint_id)].items().__reversed__():
                    if key[1] == channel_name:
                        return value[2]

            """
            Read only 1 event from the stream based on the channel version/stream position
            """
            try:
                events = self.async_client.read_stream(stream_name,
                                                 resolve_links=True,
                                                 backwards=True,
                                                 stream_position=expected_position,
                                                 limit=1).__await__()
                for event in events:
                    return self.jsonplus_serde.loads(event.data)
            except kurrentdbclient.exceptions.NotFound as e:
                return None
        return None

    async def aput_writes(
            self,
            config: RunnableConfig,
            writes: Sequence[Tuple[str, Any]],
            task_id: str,
            task_path: str = "",
    ) -> None:
        """In memory and not asynchronous"""
        self.put_writes(config, writes, task_id, task_path)

    def get_next_version(self, current: Optional[str], channel: ChannelProtocol) -> str:
        if current is None:
            current_v = 0
        elif isinstance(current, int):
            current_v = current
        else:
            current_v = int(current.split(".")[0])
        next_v = current_v + 1
        next_h = random.random()
        return f"{next_v:032}.{next_h:016}"

    #TODO: rework with new channel protocol
    def trace(self, thread_id: int):
        raise NotImplementedError("")
    def set_max_count(self, max_count: int, thread_id: int) -> None:
        raise NotImplementedError()
    def set_max_age(self, max_count: int, thread_id) -> None:
        raise NotImplementedError()

