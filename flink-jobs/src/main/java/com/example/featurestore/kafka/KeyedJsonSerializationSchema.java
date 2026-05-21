package com.example.featurestore.kafka;

import java.nio.charset.StandardCharsets;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KeyedJsonSerializationSchema<T> implements KafkaRecordSerializationSchema<T> {
    private final String topic;
    private final KeyExtractor<T> keyExtractor;
    private final JsonSerializer<T> jsonSerializer;

    public KeyedJsonSerializationSchema(
            String topic,
            KeyExtractor<T> keyExtractor,
            JsonSerializer<T> jsonSerializer
    ) {
        this.topic = topic;
        this.keyExtractor = keyExtractor;
        this.jsonSerializer = jsonSerializer;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            T element,
            KafkaSinkContext context,
            Long timestamp
    ) {
        String key = keyExtractor.extractKey(element);
        String value = jsonSerializer.toJson(element);
        return new ProducerRecord<>(
                topic,
                key == null ? null : key.getBytes(StandardCharsets.UTF_8),
                value.getBytes(StandardCharsets.UTF_8)
        );
    }
}

