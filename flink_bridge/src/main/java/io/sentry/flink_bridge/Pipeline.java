package io.sentry.flink_bridge;

import java.util.List;
import java.util.Map;

/**
 * Represents a complete pipeline configuration.
 * Contains sources, steps, and sinks.
 */
public class Pipeline {
    private Map<String, Source> sources;
    private List<PipelineStep> steps;
    private Map<String, Sink> sinks;

    /**
     * Default constructor required for YAML deserialization.
     */
    public Pipeline() {
    }

    /**
     * Constructor with parameters.
     *
     * @param sources the map of source configurations
     * @param steps   the list of pipeline steps
     * @param sinks   the map of sink configurations
     */
    public Pipeline(Map<String, Source> sources, List<PipelineStep> steps, Map<String, Sink> sinks) {
        this.sources = sources;
        this.steps = steps;
        this.sinks = sinks;
    }

    /**
     * Gets the sources map.
     *
     * @return the sources map
     */
    public Map<String, Source> getSources() {
        return sources;
    }

    public Source getSource() {
        assert sources.size() == 1;
        return sources.values().iterator().next();
    }

    /**
     * Sets the sources map.
     *
     * @param sources the sources map to set
     */
    public void setSources(Map<String, Source> sources) {
        this.sources = sources;
    }

    /**
     * Gets the steps list.
     *
     * @return the steps list
     */
    public List<PipelineStep> getSteps() {
        return steps;
    }

    /**
     * Sets the steps list.
     *
     * @param steps the steps list to set
     */
    public void setSteps(List<PipelineStep> steps) {
        this.steps = steps;
    }

    /**
     * Gets the sinks map.
     *
     * @return the sinks map
     */
    public Map<String, Sink> getSinks() {
        return sinks;
    }

    /**
     * Sets the sinks map.
     *
     * @param sinks the sinks map to set
     */
    public void setSinks(Map<String, Sink> sinks) {
        this.sinks = sinks;
    }

    @Override
    public String toString() {
        return "Pipeline{" +
                "sources=" + sources +
                ", steps=" + steps +
                ", sinks=" + sinks +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        Pipeline that = (Pipeline) o;

        if (sources != null ? !sources.equals(that.sources) : that.sources != null)
            return false;
        if (steps != null ? !steps.equals(that.steps) : that.steps != null)
            return false;
        return sinks != null ? sinks.equals(that.sinks) : that.sinks == null;
    }

    @Override
    public int hashCode() {
        int result = sources != null ? sources.hashCode() : 0;
        result = 31 * result + (steps != null ? steps.hashCode() : 0);
        result = 31 * result + (sinks != null ? sinks.hashCode() : 0);
        return result;
    }
}
