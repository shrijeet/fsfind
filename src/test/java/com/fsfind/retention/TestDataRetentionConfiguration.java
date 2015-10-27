package com.fsfind.retention;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestDataRetentionConfiguration {

    @Test(groups = { "unit" })
    public void testLoad() throws IOException {
        Map<String, DataRetentionPolicy> expected = new HashMap<String, DataRetentionPolicy>();
        Map<String, Integer> etlPaths = new HashMap<String, Integer>();
        etlPaths.put("/user/grid/path1", 10);
        etlPaths.put("/user/grid/path2", 20);
        DataRetentionPolicy etlPolicy = new DataRetentionPolicy(500, etlPaths);

        Map<String, Integer> opsPaths = new HashMap<String, Integer>();
        opsPaths.put("/user/mysql/path1", 5);
        opsPaths.put("/user/mysql/path2", 10);
        DataRetentionPolicy opsPolicy = new DataRetentionPolicy(500, opsPaths);

        expected.put("grid.etl", etlPolicy);
        expected.put("ops.mysql", opsPolicy);

        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(expected);

        File confFile = new File(Files.createTempDir(), "retention.json");
        System.out.println(confFile.getCanonicalFile());
        Files.write(json, confFile, Charsets.UTF_8);

        DataRetentionConfiguration conf = new DataRetentionConfiguration();
        Map<String, DataRetentionPolicy> actual = conf.load(confFile.getCanonicalPath());

        Assert.assertEquals(actual, expected);
        confFile.getParentFile().delete();

    }

    @Test(groups = { "unit" }, expectedExceptions = IllegalStateException.class)
    void testInvalidConf() throws IOException {
        Map<String, Integer> etlPaths = new HashMap<String, Integer>();
        DataRetentionPolicy etlPolicy = new DataRetentionPolicy(500, etlPaths);
        etlPolicy.validate();
    }

}
