# In-Memory Testing Support for Streaming Pipelines

This document describes the new in-memory testing functionality added to the rust streaming runtime, which enables unit testing and demonstration of streaming pipelines without requiring a Kafka infrastructure.

## Overview

The in-memory testing functionality allows you to:

1. **Unit test streaming pipelines** without setting up Kafka infrastructure
2. **Demo pipelines** with predefined test data  
3. **Debug and iterate** on pipeline logic quickly
4. **Test edge cases** with controlled input data

## Implementation Details

### Rust Consumer Changes

The `ArroyoConsumer` in `sentry_streams/src/consumer.rs` has been enhanced to support two modes:

#### 1. ConsumerMode Enum
```rust
pub enum ConsumerMode {
    /// Production Kafka consumer mode
    Kafka,
    /// In-memory testing mode with pre-populated data
    InMemory { 
        test_data: Vec<Vec<u8>>,
    },
}
```

#### 2. InMemoryConfig Class
```rust
#[pyclass]
pub struct InMemoryConfig {
    pub test_data: Vec<Vec<u8>>,
}
```

#### 3. New Consumer Methods
- `set_test_mode(config: InMemoryConfig)` - Switch consumer to test mode
- `is_test_mode() -> bool` - Check if consumer is in test mode
- `run_test_mode(test_data: Vec<Vec<u8>>)` - Process test data in-memory

### Python Adapter Changes

The `RustArroyoAdapter` in `sentry_streams/sentry_streams/adapters/arroyo/rust_arroyo.py` has been updated to:

#### 1. Support Test Mode Configuration
```python
class RustArroyoAdapter(StreamAdapter[Route, Route]):
    def __init__(
        self,
        steps_config: Mapping[str, StepConfig],
        test_mode: bool = False,
        test_data: list[bytes] | None = None,
    ):
        # ...
```

#### 2. Enhanced Build Method
```python
@classmethod
def build(
    cls,
    config: PipelineConfig,
    test_mode: bool = False,
    test_data: list[bytes] | None = None,
) -> Self:
    # ...
```

## Usage Examples

### Basic In-Memory Testing

```python
from sentry_streams.adapters.arroyo.rust_arroyo import RustArroyoAdapter
from sentry_streams.pipeline.chain import Map, StreamSink, streaming_source
from sentry_streams.adapters.stream_adapter import RuntimeTranslator
from sentry_streams.runner import iterate_edges

# Create test data
test_messages = [
    b"hello world",
    b"test message", 
    json.dumps({"key": "value"}).encode('utf-8'),
]

# Create pipeline
pipeline = (
    streaming_source("test_source", stream_name="test-input")
    .apply("transform", Map(lambda msg: msg.payload.upper()))
    .sink("test_sink", StreamSink(stream_name="test-output"))
)

# Configure for testing
config = {
    "steps_config": {
        "test_source": {"bootstrap_servers": ["dummy:9092"]},
        "test_sink": {"bootstrap_servers": ["dummy:9092"]},
    }
}

# Build adapter in test mode
adapter = RustArroyoAdapter.build(
    config,
    test_mode=True,
    test_data=test_messages
)

# Build and run pipeline
iterate_edges(pipeline, RuntimeTranslator(adapter))
adapter.run()  # Processes test data in-memory
```

### Unit Testing with Assertions

```python
import unittest
from sentry_streams.rust_streams import InMemoryConfig, ArroyoConsumer

class TestStreamingPipeline(unittest.TestCase):
    
    def test_message_transformation(self):
        # Setup test data
        config = InMemoryConfig()
        config.add_test_data([b"input1", b"input2"])
        
        # Create consumer and set test mode
        consumer = ArroyoConsumer(
            source="test",
            kafka_config=dummy_kafka_config,
            topic="test-topic", 
            schema="test-schema"
        )
        consumer.set_test_mode(config)
        
        # Add transformation steps
        consumer.add_step(map_operator)
        consumer.add_step(filter_operator)
        
        # Run and verify
        self.assertTrue(consumer.is_test_mode())
        consumer.run()  # Processes test data
        
        # Add assertions here based on expected output
```

## Architecture Benefits

### 1. **Faster Test Execution**
- No Kafka broker setup/teardown
- No network I/O overhead
- Direct in-memory message processing

### 2. **Deterministic Testing** 
- Controlled input data
- Reproducible test scenarios
- No external dependencies

### 3. **Easy Debugging**
- Step-by-step processing visibility
- Console logging of message flow
- Simplified error isolation

### 4. **Development Workflow**
- Rapid iteration on pipeline logic
- Quick validation of transformations
- Easy demonstration of functionality

## Comparison with Kafka Mode

| Aspect | Kafka Mode | In-Memory Mode |
|--------|------------|----------------|
| **Setup** | Requires Kafka broker | No external dependencies |
| **Speed** | Network I/O overhead | Direct memory access |
| **Data** | Stream processing | Batch processing of test data |
| **State** | Persistent offsets | Stateless execution |
| **Scale** | Production workloads | Testing and demos |
| **Debugging** | Complex distributed system | Simple single-process |

## Limitations

1. **No Persistence** - Test data is lost when process ends
2. **No Checkpointing** - Consumer always starts from beginning 
3. **Single Container** - No multi-process testing support
4. **Bounded Data** - Only supports predefined test datasets
5. **Local Only** - Cannot be used in distributed/remote environments

## Migration Guide

### From Kafka Testing to In-Memory

**Before:**
```python
# Required Kafka broker setup
broker = LocalBroker(MemoryMessageStorage(), MockedClock())
adapter = ArroyoAdapter.build(config, sources_override={...})
```

**After:**
```python
# No broker needed
adapter = RustArroyoAdapter.build(
    config, 
    test_mode=True, 
    test_data=[b"test1", b"test2"]
)
```

### Adding Test Mode to Existing Pipelines

1. **Update adapter creation** to include `test_mode=True`
2. **Provide test_data** as list of byte strings
3. **Remove Kafka broker dependencies** from test setup
4. **Simplify test assertions** to focus on business logic

## Future Enhancements

1. **Output Capture** - Capture processed messages for verification
2. **Multi-Consumer Support** - Test pipelines with multiple sources
3. **Streaming Simulation** - Add timing/delay simulation 
4. **State Testing** - Support for stateful operations testing
5. **Error Injection** - Simulate failures for robustness testing

## Related Files

- `sentry_streams/src/consumer.rs` - Core Rust implementation
- `sentry_streams/src/lib.rs` - Python module exports
- `sentry_streams/sentry_streams/adapters/arroyo/rust_arroyo.py` - Python adapter
- `sentry_streams/examples/in_memory_test_example.py` - Usage example
- Tests in `sentry_streams/src/consumer.rs` - Unit tests for functionality