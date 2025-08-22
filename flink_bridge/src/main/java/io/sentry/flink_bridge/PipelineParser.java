package io.sentry.flink_bridge;

import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Parser for YAML pipeline configuration files.
 * Reads YAML files and converts them to Pipeline objects.
 * Supports the new format with sources, steps, and sinks.
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
     * Parses a YAML file and returns a Pipeline object.
     *
     * @param filename the path to the YAML file
     * @return a Pipeline object
     * @throws FileNotFoundException if the file is not found
     * @throws RuntimeException      if there's an error parsing the YAML
     */
    public Pipeline parseFile(String filename) throws FileNotFoundException {
        try (FileInputStream inputStream = new FileInputStream(new File(filename))) {
            return parseInputStream(inputStream);
        } catch (FileNotFoundException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Error parsing YAML file: " + filename, e);
        }
    }

    /**
     * Parses YAML content from an InputStream and returns a Pipeline object.
     *
     * @param inputStream the InputStream containing YAML content
     * @return a Pipeline object
     * @throws RuntimeException if there's an error parsing the YAML
     */
    public Pipeline parseInputStream(InputStream inputStream) {
        try {
            Object rawPipeline = yaml.load(inputStream);

            if (rawPipeline == null) {
                throw new RuntimeException("YAML file is empty or could not be parsed");
            }

            if (!(rawPipeline instanceof Map)) {
                throw new RuntimeException("YAML file must contain a map with sources, steps, and/or sinks");
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> pipelineMap = (Map<String, Object>) rawPipeline;

            return parsePipeline(pipelineMap);
        } catch (Exception e) {
            throw new RuntimeException("Error parsing YAML content", e);
        }
    }

    /**
     * Parses the pipeline configuration.
     *
     * @param pipelineMap the parsed YAML map
     * @return a Pipeline object
     */
    private Pipeline parsePipeline(Map<String, Object> pipelineMap) {
        Map<String, Source> sources = parseSources(pipelineMap.get("sources"));
        List<PipelineStep> steps = parseSteps(pipelineMap.get("steps"));
        Map<String, Sink> sinks = parseSinks(pipelineMap.get("sinks"));

        return new Pipeline(sources, steps, sinks);
    }

    /**
     * Parses sources from the raw YAML data.
     *
     * @param rawSources the raw sources data
     * @return a map of source configurations
     */
    @SuppressWarnings("unchecked")
    private Map<String, Source> parseSources(Object rawSources) {
        Map<String, Source> sources = new HashMap<>();

        if (rawSources == null) {
            return sources;
        }

        Map<String, Object> sourcesMap = (Map<String, Object>) rawSources;
        for (Map.Entry<String, Object> entry : sourcesMap.entrySet()) {
            String sourceName = entry.getKey();
            @SuppressWarnings("unchecked")
            Map<String, Object> sourceConfig = (Map<String, Object>) entry.getValue();

            if (sourceConfig == null) {
                sourceConfig = new HashMap<>();
            }

            sources.put(sourceName, new Source(sourceName, sourceConfig));
        }

        return sources;
    }

    /**
     * Parses sinks from the raw YAML data.
     *
     * @param rawSinks the raw sinks data
     * @return a map of sink configurations
     */
    @SuppressWarnings("unchecked")
    private Map<String, Sink> parseSinks(Object rawSinks) {
        Map<String, Sink> sinks = new HashMap<>();

        if (rawSinks == null) {
            return sinks;
        }

        Map<String, Object> sinksMap = (Map<String, Object>) rawSinks;
        for (Map.Entry<String, Object> entry : sinksMap.entrySet()) {
            String sinkName = entry.getKey();
            @SuppressWarnings("unchecked")
            Map<String, Object> sinkConfig = (Map<String, Object>) entry.getValue();

            if (sinkConfig == null) {
                sinkConfig = new HashMap<>();
            }

            sinks.put(sinkName, new Sink(sinkName, sinkConfig));
        }

        return sinks;
    }

    /**
     * Parses steps from the raw YAML data.
     *
     * @param rawSteps the raw steps data
     * @return a list of PipelineStep objects
     */
    @SuppressWarnings("unchecked")
    private List<PipelineStep> parseSteps(Object rawSteps) {
        List<PipelineStep> steps = new ArrayList<>();

        if (rawSteps == null) {
            return steps;
        }

        List<Map<String, Object>> stepsList = (List<Map<String, Object>>) rawSteps;
        for (Map<String, Object> rawStep : stepsList) {
            String stepName = (String) rawStep.get("step_name");
            @SuppressWarnings("unchecked")
            Map<String, Object> config = (Map<String, Object>) rawStep.get("config");

            if (stepName == null) {
                throw new RuntimeException("Missing 'step_name' field in step: " + rawStep);
            }

            if (config == null) {
                config = new HashMap<>(); // Empty config if not present
            }

            steps.add(new PipelineStep(stepName, config));
        }

        return steps;
    }
}
