package io.sentry.flink_bridge;

import java.util.Map;

/**
 * Represents a sink configuration in a pipeline.
 * Each sink has a name and a configuration map.
 */
public class Sink {
    private String name;
    private Map<String, Object> config;

    /**
     * Default constructor required for YAML deserialization.
     */
    public Sink() {
    }

    /**
     * Constructor with parameters.
     *
     * @param name   the name of the sink
     * @param config the configuration map for the sink
     */
    public Sink(String name, Map<String, Object> config) {
        this.name = name;
        this.config = config;
    }

    /**
     * Gets the sink name.
     *
     * @return the sink name
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the sink name.
     *
     * @param name the sink name to set
     */
    public void setName(String name) {
        this.name = name;
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
        return "Sink{" +
                "name='" + name + '\'' +
                ", config=" + config +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        Sink that = (Sink) o;

        if (name != null ? !name.equals(that.name) : that.name != null)
            return false;
        return config != null ? config.equals(that.config) : that.config != null;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (config != null ? config.hashCode() : 0);
        return result;
    }
}
