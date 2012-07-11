package com.sonar.expedition.scrawler.json;

import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.PropertyNamingStrategy;
import org.codehaus.jackson.map.annotate.JsonSerialize;

public class CustomObjectMapper extends ObjectMapper {

    private static ObjectMapper instance;

    public CustomObjectMapper() {
        setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);
        setSerializationInclusion(JsonSerialize.Inclusion.NON_NULL);
        disable(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    public static ObjectMapper getInstance() {
        if (instance == null) {
            instance = new CustomObjectMapper();
        }
        return instance;
    }
}
