package com.example.featurestore.serde;

import com.example.featurestore.model.DlqEvent;
import com.example.featurestore.model.FeatureUpdate;
import com.example.featurestore.model.UserEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public final class JsonSerdes {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private JsonSerdes() {
    }

    public static UserEvent parseUserEvent(String rawJson) throws JsonProcessingException {
        return MAPPER.readValue(rawJson, UserEvent.class);
    }

    public static String toJson(UserEvent event) throws JsonProcessingException {
        return MAPPER.writeValueAsString(event);
    }

    public static String toJson(DlqEvent event) throws JsonProcessingException {
        return MAPPER.writeValueAsString(event);
    }

    public static String toJson(FeatureUpdate update) throws JsonProcessingException {
        return MAPPER.writeValueAsString(update);
    }

    public static String objectToJson(Object value) throws JsonProcessingException {
        return MAPPER.writeValueAsString(value);
    }
}
