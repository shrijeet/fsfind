package com.sp;

import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.Map;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * <code>DataRetentionPolicy</code> represents a retention policy in JSON format. Policy contains
 * paths & their corresponding retention time periods & attributes like batch delete size etc.
 */
public class DataRetentionPolicy implements Serializable {
    private int batchSize;
    private Map<String, Integer> pathMapping;

    /**
     * @param batch the batch delete size
     * @param pathMapping paths and their retention periods
     */
    public DataRetentionPolicy(
            @JsonProperty("batchSize")
            int batch,
            @JsonProperty("pathMapping")
            Map<String, Integer> pathMapping) {
        this.batchSize = batch;
        this.pathMapping = pathMapping;
    }

    /**
     * @return the configured batch delete size
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * @return mapping of paths & their corresponding retention periods
     */
    public Map<String, Integer> getPathMapping() {
        return pathMapping;
    }

    /**
     * Validate the policy, throw a RuntimeException if its not valid.
     *
     * @return true if polciy is valid
     */
    public boolean validate() {
        Preconditions.checkState(batchSize >= 0, "batchSize can't be negative");
        Preconditions.checkState(!pathMapping.isEmpty(), "no path mapping found");
        return true;
    }

    /**
     * Override the configured batching and set it to Integer.MAX_VALUE, essentially turning
     * batching off.
     */
    public void turnOffBatching() {
        this.batchSize = Integer.MAX_VALUE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DataRetentionPolicy)) {
            return false;
        }

        DataRetentionPolicy that = (DataRetentionPolicy) o;

        if (batchSize != that.batchSize) {
            return false;
        }
        if (!pathMapping.equals(that.pathMapping)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = batchSize;
        result = 31 * result + pathMapping.hashCode();
        return result;
    }
}
