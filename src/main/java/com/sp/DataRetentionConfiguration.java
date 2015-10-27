package com.sp;

import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.type.MapType;

/**
 * <code>DataRetentionConfiguration</code> is programmatic representation of JSON configuration file
 * containing multiple <code>DataRetentionPolicy</code> keyed by their names.
 */
public class DataRetentionConfiguration {

    private final ObjectMapper mapper = new ObjectMapper();

    /**
     * Load the configuration file.
     *
     * @param file path of configuration file
     * @return a map of data retention policies keyed by their names
     * @throws IOException
     */
    public Map<String, DataRetentionPolicy> load(String file) throws IOException {
        // read the file and escape all forward slashes before deserializing.
        String rawJson = new String(Files.toByteArray(new File(file)));
        String escapedJson = rawJson.replaceAll("/", Matcher.quoteReplacement("\\/"));
        MapType mapType = mapper.getTypeFactory().constructMapType(HashMap.class, String.class,
                DataRetentionPolicy.class);
        Map<String, DataRetentionPolicy> policyMap = mapper.readValue(escapedJson, mapType);
        return policyMap;
    }
}
