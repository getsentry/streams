# Pipeline Parser

This module provides Java classes for parsing YAML pipeline configuration files produced by the FlinkPipelineAdapter.

## Overview

The pipeline parser consists of two main classes:
- `PipelineStep`: A POJO representing a single pipeline step
- `PipelineParser`: A utility class for parsing YAML files into lists of PipelineStep objects

## YAML Format

The parser expects YAML files with the following structure:

```yaml
- config: {}
  step_name: parser
- config:
    batch_size: 2
    batch_timedelta:18c0a70726f6a6
      seconds: 20
  step_name: mybatchk
```

Each step must have:
- `step_name`: A string identifying the step
- `config`: A map containing configuration parameters (can be empty)

## Usage

### Basic Parsing

```java
import io.sentry.flink_bridge.PipelineParser;
import io.sentry.flink_bridge.PipelineStep;
import java.util.List;

// Create parser
PipelineParser parser = new PipelineParser();

// Parse from file
List<PipelineStep> steps = parser.parseFile("pipeline_config.yaml");

// Parse from string
String yamlContent = "...";
List<PipelineStep> steps = parser.parseString(yamlContent);

// Access step information
for (PipelineStep step : steps) {
    System.out.println("Step: " + step.getStepName());
    System.out.println("Config: " + step.getConfig());
}
```

### Working with PipelineStep Objects

```java
PipelineStep step = steps.get(0);

// Get step name
String stepName = step.getStepName();

// Get configuration
Map<String, Object> config = step.getConfig();

// Access specific config values
if (config.containsKey("batch_size")) {
    Integer batchSize = (Integer) config.get("batch_size");
    System.out.println("Batch size: " + batchSize);
}
```

## Dependencies

The parser requires the SnakeYAML library, which is already included in the project's `pom.xml`:

```xml
<dependency>
    <groupId>org.yaml</groupId>
    <artifactId>snakeyaml</artifactId>
    <version>2.2</version>
</dependency>
```

## Error Handling

The parser provides comprehensive error handling:

- `FileNotFoundException`: Thrown when the specified file doesn't exist
- `RuntimeException`: Thrown for parsing errors, empty YAML, or other issues

```java
try {
    List<PipelineStep> steps = parser.parseFile("config.yaml");
    // Process steps...
} catch (FileNotFoundException e) {
    System.err.println("File not found: " + e.getMessage());
} catch (RuntimeException e) {
    System.err.println("Parsing error: " + e.getMessage());
}
```

## Examples

See `PipelineParserExample.java` for complete usage examples including:
- File parsing
- String parsing
- Business logic processing

## Testing

Run the tests with:

```bash
mvn test -Dtest=PipelineParserTest
```

The tests verify:
- Correct parsing of valid YAML
- Error handling for invalid YAML
- Edge cases like empty content

## Integration with FlinkPipelineAdapter

This parser is designed to work with the YAML output from the Python `FlinkPipelineAdapter` class. The adapter produces YAML descriptions that can be directly consumed by this Java parser, enabling seamless integration between Python pipeline generation and Java pipeline execution.
