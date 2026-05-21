package com.example.featurestore.kafka;

import java.io.Serializable;

@FunctionalInterface
public interface KeyExtractor<T> extends Serializable {
    String extractKey(T value);
}

