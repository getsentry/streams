package io.sentry.flink_bridge;

import java.util.Map;

/**
 * Represents a single step in a pipeline configuration.
 * Each step has a name and a configuration map.
 */
public class PipelineStep {
    private String stepName;
    private Map<String, Object> config;

    /**
     * Default constructor required for YAML deserialization.
     */
    public PipelineStep() {
    }

    /**
     * Constructor with parameters.
     *
     * @param stepName the name of the step
     * @param config   the configuration map for the step
     */
    public PipelineStep(String stepName, Map<String, Object> config) {
        this.stepName = stepName;
        this.config = config;
    }

    /**
     * Gets the step name.
     *
     * @return the step name
     */
    public String getStepName() {
        return stepName;
    }

    /**
     * Sets the step name.
     *
     * @param stepName the step name to set
     */
    public void setStepName(String stepName) {
        this.stepName = stepName;
    }

    /**
     * Gets the configuration map.
     *
     * @return the configuration map
     */
    public Map<String, Object> getConfig() {
        return config;
    }

    /**
     * Sets the configuration map.
     *
     * @param config the configuration map to set
     */
    public void setConfig(Map<String, Object> config) {
        this.config = config;
    }

    @Override
    public String toString() {
        return "PipelineStep{" +
                "stepName='" + stepName + '\'' +
                ", config=" + config +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        PipelineStep that = (PipelineStep) o;

        if (stepName != null ? !stepName.equals(that.stepName) : that.stepName != null)
            return false;
        return config != null ? config.equals(that.config) : that.config == null;
    }

    @Override
    public int hashCode() {
        int result = stepName != null ? stepName.hashCode() : 0;
        result = 31 * result + (config != null ? config.hashCode() : 0);
        return result;
    }
}
