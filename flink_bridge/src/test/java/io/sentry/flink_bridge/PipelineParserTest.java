package io.sentry.flink_bridge;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Test class for PipelineParser.
 */
public class PipelineParserTest {

    private PipelineParser parser;

    @BeforeEach
    void setUp() {
        parser = new PipelineParser();
    }

    @Test
    void testParseNewFormat() throws IOException {
        String yamlContent = """
                sources:
                  kafka1:
                    bootstrap_servers: ["127.0.0.1:9092"]
                    topic: "input-topic"
                steps:
                  - config: {}
                    step_name: parser
                  - config:
                      batch_size: 2
                      batch_timedelta:
                        seconds: 20
                    step_name: mybatch
                sinks:
                  kafkasink1:
                    bootstrap_servers: ["127.0.0.1:9092"]
                    topic: "output-topic"
                """;

        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(yamlContent.getBytes())) {
            Pipeline pipeline = parser.parseInputStream(inputStream);

            // Test sources
            assertNotNull(pipeline.getSources());
            assertEquals(1, pipeline.getSources().size());
            assertTrue(pipeline.getSources().containsKey("kafka1"));
            Source kafka1 = pipeline.getSources().get("kafka1");
            assertEquals("kafka1", kafka1.getName());
            assertEquals("input-topic", kafka1.getConfig().get("topic"));

            // Test steps
            assertNotNull(pipeline.getSteps());
            assertEquals(2, pipeline.getSteps().size());

            PipelineStep parserStep = pipeline.getSteps().get(0);
            assertEquals("parser", parserStep.getStepName());
            assertTrue(parserStep.getConfig().isEmpty());

            PipelineStep batchStep = pipeline.getSteps().get(1);
            assertEquals("mybatch", batchStep.getStepName());
            assertEquals(2, batchStep.getConfig().get("batch_size"));

            @SuppressWarnings("unchecked")
            Map<String, Object> batchTimedelta = (Map<String, Object>) batchStep.getConfig().get("batch_timedelta");
            assertEquals(20, batchTimedelta.get("seconds"));

            // Test sinks
            assertNotNull(pipeline.getSinks());
            assertEquals(1, pipeline.getSinks().size());
            assertTrue(pipeline.getSinks().containsKey("kafkasink1"));
            Sink kafkaSink = pipeline.getSinks().get("kafkasink1");
            assertEquals("kafkasink1", kafkaSink.getName());
            assertEquals("output-topic", kafkaSink.getConfig().get("topic"));
        }
    }

    @Test
    void testParseWithEmptySourcesAndSinks() throws IOException {
        String yamlContent = """
                sources: {}
                steps:
                  - config: {}
                    step_name: parser
                sinks: {}
                """;

        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(yamlContent.getBytes())) {
            Pipeline pipeline = parser.parseInputStream(inputStream);

            assertNotNull(pipeline.getSources());
            assertTrue(pipeline.getSources().isEmpty());

            assertNotNull(pipeline.getSinks());
            assertTrue(pipeline.getSinks().isEmpty());

            assertNotNull(pipeline.getSteps());
            assertEquals(1, pipeline.getSteps().size());
        }
    }

    @Test
    void testParseWithMissingSourcesAndSinks() throws IOException {
        String yamlContent = """
                steps:
                  - config: {}
                    step_name: parser
                """;

        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(yamlContent.getBytes())) {
            Pipeline pipeline = parser.parseInputStream(inputStream);

            assertNotNull(pipeline.getSources());
            assertTrue(pipeline.getSources().isEmpty());

            assertNotNull(pipeline.getSinks());
            assertTrue(pipeline.getSinks().isEmpty());

            assertNotNull(pipeline.getSteps());
            assertEquals(1, pipeline.getSteps().size());
        }
    }

    @Test
    void testParseWithOnlySources() throws IOException {
        String yamlContent = """
                sources:
                  kafka1:
                    bootstrap_servers: ["127.0.0.1:9092"]
                    topic: "input-topic"
                """;

        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(yamlContent.getBytes())) {
            Pipeline pipeline = parser.parseInputStream(inputStream);

            assertNotNull(pipeline.getSources());
            assertEquals(1, pipeline.getSources().size());
            assertTrue(pipeline.getSources().containsKey("kafka1"));

            assertNotNull(pipeline.getSinks());
            assertTrue(pipeline.getSinks().isEmpty());

            assertNotNull(pipeline.getSteps());
            assertTrue(pipeline.getSteps().isEmpty());
        }
    }

    @Test
    void testParseWithOnlySinks() throws IOException {
        String yamlContent = """
                sinks:
                  kafkasink1:
                    bootstrap_servers: ["127.0.0.1:9092"]
                    topic: "output-topic"
                """;

        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(yamlContent.getBytes())) {
            Pipeline pipeline = parser.parseInputStream(inputStream);

            assertNotNull(pipeline.getSources());
            assertTrue(pipeline.getSources().isEmpty());

            assertNotNull(pipeline.getSinks());
            assertEquals(1, pipeline.getSinks().size());
            assertTrue(pipeline.getSinks().containsKey("kafkasink1"));

            assertNotNull(pipeline.getSteps());
            assertTrue(pipeline.getSteps().isEmpty());
        }
    }

    @Test
    void testParseWithOnlySteps() throws IOException {
        String yamlContent = """
                steps:
                  - config: {}
                    step_name: parser
                  - config:
                      batch_size: 2
                    step_name: mybatch
                """;

        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(yamlContent.getBytes())) {
            Pipeline pipeline = parser.parseInputStream(inputStream);

            assertNotNull(pipeline.getSources());
            assertTrue(pipeline.getSources().isEmpty());

            assertNotNull(pipeline.getSinks());
            assertTrue(pipeline.getSinks().isEmpty());

            assertNotNull(pipeline.getSteps());
            assertEquals(2, pipeline.getSteps().size());
            assertEquals("parser", pipeline.getSteps().get(0).getStepName());
            assertEquals("mybatch", pipeline.getSteps().get(1).getStepName());
        }
    }

    @Test
    void testParseInvalidFormat() {
        String yamlContent = """
                - config: {}
                  step_name: parser
                """;

        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(yamlContent.getBytes())) {
            RuntimeException exception = assertThrows(RuntimeException.class, () -> {
                parser.parseInputStream(inputStream);
            });
            // Check that the error message contains the expected text
            assertTrue(exception.getMessage().contains("Error parsing YAML content"));
            // Check that the cause contains the specific error message
            assertTrue(exception.getCause().getMessage()
                    .contains("YAML file must contain a map with sources, steps, and/or sinks"));
        } catch (IOException e) {
            fail("IOException should not occur");
        }
    }
}
