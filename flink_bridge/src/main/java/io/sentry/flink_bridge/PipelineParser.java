package io.sentry.flink_bridge;

import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;

/**
 * Parser for YAML pipeline configuration files.
 * Reads YAML files and converts them to a list of PipelineStep objects.
 */
public class PipelineParser {

    private final Yaml yaml;

    /**
     * Default constructor that initializes the YAML parser.
     */
    public PipelineParser() {
        this.yaml = new Yaml();
    }

    /**
     * Parses a YAML file and returns a list of PipelineStep objects.
     *
     * @param filename the path to the YAML file
     * @return a list of PipelineStep objects
     * @throws FileNotFoundException if the file is not found
     * @throws RuntimeException      if there's an error parsing the YAML
     */
    public List<PipelineStep> parseFile(String filename) throws FileNotFoundException {
        try (FileInputStream inputStream = new FileInputStream(new File(filename))) {
            return parseInputStream(inputStream);
        } catch (FileNotFoundException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Error parsing YAML file: " + filename, e);
        }
    }

    /**
     * Parses YAML content from an InputStream and returns a list of PipelineStep
     * objects.
     *
     * @param inputStream the InputStream containing YAML content
     * @return a list of PipelineStep objects
     * @throws RuntimeException if there's an error parsing the YAML
     */
    public List<PipelineStep> parseInputStream(InputStream inputStream) {
        try {
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> rawSteps = (List<Map<String, Object>>) yaml.load(inputStream);

            if (rawSteps == null) {
                throw new RuntimeException("YAML file is empty or could not be parsed");
            }

            List<PipelineStep> steps = new ArrayList<>();
            for (Map<String, Object> rawStep : rawSteps) {
                String stepName = (String) rawStep.get("step_name");
                @SuppressWarnings("unchecked")
                Map<String, Object> config = (Map<String, Object>) rawStep.get("config");

                if (stepName == null) {
                    throw new RuntimeException("Missing 'step_name' field in step: " + rawStep);
                }

                if (config == null) {
                    config = Map.of(); // Empty config if not present
                }

                steps.add(new PipelineStep(stepName, config));
            }

            return steps;
        } catch (Exception e) {
            throw new RuntimeException("Error parsing YAML content", e);
        }
    }
}
