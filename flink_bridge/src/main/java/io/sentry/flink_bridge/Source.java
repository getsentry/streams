package io.sentry.flink_bridge;

import java.util.Map;

/**
 * Represents a source configuration in a pipeline.
 * Each source has a name and a configuration map.
 */
public class Source {
    private String name;
    private Map<String, Object> config;

    /**
     * Default constructor required for YAML deserialization.
     */
    public Source() {
    }

    /**
     * Constructor with parameters.
     *
     * @param name   the name of the source
     * @param config the configuration map for the source
     */
    public Source(String name, Map<String, Object> config) {
        this.name = name;
        this.config = config;
    }

    /**
     * Gets the source name.
     *
     * @return the source name
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the source name.
     *
     * @param name the source name to set
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
        return "Source{" +
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

        Source that = (Source) o;

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
