package com.sonar.expedition.scrawler.json;

//import org.codehaus.jackson.map.DeserializationConfig;
//import org.codehaus.jackson.map.ObjectMapper;
//import org.codehaus.jackson.map.PropertyNamingStrategy;
//import org.codehaus.jackson.map.annotate.JsonSerialize;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

public class CustomObjectMapper extends ObjectMapper {

    private static ObjectMapper instance;

    public CustomObjectMapper() {
        setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);
        this.getDeserializationConfig().without(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES);
        this.getSerializationConfig().without(SerializationFeature.FAIL_ON_EMPTY_BEANS);
    }

    public static ObjectMapper getInstance() {
        if (instance == null) {
            instance = new CustomObjectMapper();
        }
        return instance;
    }
}
