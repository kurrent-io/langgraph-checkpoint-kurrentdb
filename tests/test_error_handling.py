import pytest
from unittest.mock import MagicMock, patch
import datetime
from langgraph.graph import StateGraph
from langgraph_checkpoint_kurrentdb import KurrentDBSaver
from kurrentdbclient import exceptions, KurrentDBClient, AsyncKurrentDBClient

# Mock Classes
class MockClient:
    def __init__(self, fail_on=None):
        """
        fail_on: A list of method names that should raise exceptions when called
        """
        self.fail_on = fail_on or []
        self.calls = {}  # Track method calls

    def get_stream(self, stream_name, **kwargs):
        self._track_call("get_stream", stream_name=stream_name, **kwargs)
        if "get_stream" in self.fail_on:
            raise exceptions.NotFound(f"Stream {stream_name} not found")
        elif "error_stream" in stream_name:
            raise exceptions.ConnectionFailed("Mock connection failure")
        elif "empty_stream" in stream_name:
            return []
        else:
            # Create a mock event
            event = MagicMock()
            event.data = b'{"id":"mock-id","ts":"2024-05-05T10:00:00","channel_values":{"mock":"data"}}'
            event.metadata = b'{"source":"mock"}'
            event.stream_name = stream_name
            event.recorded_at = datetime.datetime.now()
            return [event]

    def append_to_stream(self, stream_name, events, **kwargs):
        self._track_call("append_to_stream", stream_name=stream_name, events=events, **kwargs)
        if "append_to_stream" in self.fail_on:
            raise exceptions.WrongExpectedVersion("Mock version conflict")
        elif "error_stream" in stream_name:
            raise exceptions.ConnectionFailed("Mock connection failure")
        return True

    def set_stream_metadata(self, stream_name, metadata):
        self._track_call("set_stream_metadata", stream_name=stream_name, metadata=metadata)
        if "set_stream_metadata" in self.fail_on:
            raise exceptions.AccessDenied("Mock access denied")
        return True

    def _track_call(self, method, **kwargs):
        if method not in self.calls:
            self.calls[method] = []
        self.calls[method].append(kwargs)

# Fixtures
@pytest.fixture
def mock_client():
    return MockClient()

@pytest.fixture
def failing_client():
    return MockClient(fail_on=["get_stream", "append_to_stream", "set_stream_metadata"])

@pytest.fixture
def mock_saver(mock_client):
    return KurrentDBSaver(client=mock_client)

@pytest.fixture
def failing_saver(failing_client):
    return KurrentDBSaver(client=failing_client)

@pytest.fixture
def base_config():
    return {"configurable": {"thread_id": "error_test", "checkpoint_ns": "error_ns"}}

@pytest.fixture
def sample_checkpoint():
    return {
        "ts": "2024-05-05T10:00:00",
        "id": "error-checkpoint-id",
        "channel_values": {"test_key": "test_value"}
    }

@pytest.fixture
def sample_metadata():
    return {"source": "error_test"}

# Tests for error handling
def test_notfound_exception_handling(mock_saver, base_config):
    """Test that NotFound exceptions are properly handled in get_tuple"""
    config_error = {"configurable": {"thread_id": "non_existent_thread", "checkpoint_ns": ""}}

    # self.client should raise exception notfound and then return None
    with patch.object(mock_saver.client, 'get_stream', side_effect=exceptions.NotFound("Stream not found")):
        result = mock_saver.get_tuple(config_error)
        assert result is None

def test_missing_client_error(base_config):
    """Test error when no client is provided"""
    with pytest.raises(Exception) as excinfo:
        KurrentDBSaver()  # No client provided
    assert "At least one of sync or async client must be provided" in str(excinfo.value)

def test_sync_client_required(async_client, base_config):
    """Test error when sync method is called but only async client is provided"""
    saver = KurrentDBSaver(async_client=async_client)
    
    with pytest.raises(Exception) as excinfo:
        saver.get_tuple(base_config)
    assert "Synchronous Client is required" in str(excinfo.value)

@pytest.mark.asyncio
async def test_async_client_required(client, base_config):
    """Test error when async method is called but only sync client is provided"""
    saver = KurrentDBSaver(client=client)
    
    with pytest.raises(Exception) as excinfo:
        await saver.aget_tuple(base_config)
    assert "ASynchronous Client is required" in str(excinfo.value)

# Tests with patched components
@patch('langgraph_checkpoint_kurrentdb.JsonPlusSerializer')
def test_serialization_error(mock_serializer, client, base_config, sample_checkpoint, sample_metadata):
    """Test handling of serialization errors"""
    # Setup the mock serializer to fail on dumps
    serializer_instance = MagicMock()
    serializer_instance.dumps.side_effect = Exception("Mock serialization error")
    mock_serializer.return_value = serializer_instance
    
    # Create saver with the mocked serializer
    saver = KurrentDBSaver(client=client)
    
    # Should raise the serialization error
    with pytest.raises(Exception) as excinfo:
        saver.put(base_config, sample_checkpoint, sample_metadata, {})
    assert "Mock serialization error" in str(excinfo.value)

def test_invalid_checkpoint_structure(mock_saver, base_config):
    """Test behavior with malformed checkpoint data"""
    # Create an invalid checkpoint and metadata
    invalid_checkpoint = {"no_id_field": "missing id", "ts": "2024-05-05T10:00:00"}
    invalid_metadata = "not a dict"  # Should be a dict
    
    # This should raise a TypeError or similar when trying to access non-existent keys
    with pytest.raises(Exception):
        mock_saver.put(base_config, invalid_checkpoint, invalid_metadata, {})

def test_empty_stream_handling(mock_saver):
    """Test behavior when stream exists but is empty"""
    empty_config = {"configurable": {"thread_id": "empty_stream", "checkpoint_ns": ""}}
    
    # get_tuple should return None for an empty stream
    assert mock_saver.get_tuple(empty_config) is None

