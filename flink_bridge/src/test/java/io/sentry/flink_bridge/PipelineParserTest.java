package io.sentry.flink_bridge;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

/**
 * Test class for PipelineParser.
 */
public class PipelineParserTest {

    private PipelineParser parser;
    private File tempYamlFile;

    @BeforeEach
    void setUp() throws IOException {
        parser = new PipelineParser();

        // Create a temporary YAML file with the example content
        tempYamlFile = File.createTempFile("pipeline", ".yaml");
        tempYamlFile.deleteOnExit();

        String yamlContent = """
                - config: {}
                  step_name: parser
                - config:
                    batch_size: 2
                    batch_timedelta: "18c0a70726f6a6"
                    seconds: 20
                  step_name: mybatchk
                """;

        try (FileWriter writer = new FileWriter(tempYamlFile)) {
            writer.write(yamlContent);
        }
    }

    @Test
    void testParseFile() throws Exception {
        List<PipelineStep> steps = parser.parseFile(tempYamlFile.getPath());

        assertNotNull(steps);
        assertEquals(2, steps.size());

        // Check first step
        PipelineStep firstStep = steps.get(0);
        assertEquals("parser", firstStep.getStepName());
        assertNotNull(firstStep.getConfig());
        assertTrue(firstStep.getConfig().isEmpty());

        // Check second step
        PipelineStep secondStep = steps.get(1);
        assertEquals("mybatchk", secondStep.getStepName());
        assertNotNull(secondStep.getConfig());
        assertEquals(2, secondStep.getConfig().get("batch_size"));
        assertEquals(20, secondStep.getConfig().get("seconds"));
    }

}
