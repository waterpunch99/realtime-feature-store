package com.example.featurestore.kafka;

import java.io.Serializable;

@FunctionalInterface
public interface JsonSerializer<T> extends Serializable {
    String toJson(T value);
}

