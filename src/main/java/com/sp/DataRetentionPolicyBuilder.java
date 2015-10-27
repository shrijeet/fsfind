package com.sp;

import java.util.HashMap;
import java.util.Map;

public class DataRetentionPolicyBuilder {
    private final String hdfsPath;
    private final int numDays;
    private final int batchSize;

    public DataRetentionPolicyBuilder(String hdfsPath, int numDays, int batchSize) {
        this.hdfsPath = hdfsPath;
        this.numDays = numDays;
        this.batchSize = batchSize;
    }

    public DataRetentionPolicy create() {
        Map<String, Integer> givenPath = new HashMap<String, Integer>();
        givenPath.put(hdfsPath, numDays);
        return new DataRetentionPolicy(batchSize, givenPath);
    }
}
