package com.example.featurestore.jobs;

import com.example.featurestore.config.JobConfig;
import com.example.featurestore.kafka.KeyedJsonSerializationSchema;
import com.example.featurestore.model.DlqEvent;
import com.example.featurestore.model.UserEvent;
import com.example.featurestore.serde.JsonSerdes;
import com.example.featurestore.validation.EventValidator;
import com.example.featurestore.validation.ValidationResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

public class ValidationEnrichmentJob {
    public static void main(String[] args) throws Exception {
        JobConfig config = JobConfig.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(
                org.apache.flink.api.java.utils.ParameterTool.fromArgs(args)
        );

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setTopics(config.rawTopic())
                .setGroupId(config.consumerGroupId())
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> rawEvents = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "raw-user-events-source"
        );

        DataStream<ValidationResult> validationResults = rawEvents
                .map(new ValidationMapFunction())
                .name("validate-and-enrich");

        DataStream<UserEvent> cleanEvents = validationResults
                .filter(ValidationResult::isValid)
                .map(ValidationResult::getEvent)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<UserEvent>forBoundedOutOfOrderness(
                                        Duration.ofSeconds(config.watermarkDelaySeconds())
                                )
                                .withTimestampAssigner(
                                        (SerializableTimestampAssigner<UserEvent>)
                                                (event, timestamp) -> event.getEventTime().toEpochMilli()
                                )
                )
                .name("clean-events-with-event-time-watermark");

        DataStream<DlqEvent> invalidEvents = validationResults
                .filter(result -> !result.isValid())
                .map(ValidationResult::getDlqEvent)
                .name("invalid-events");

        cleanEvents.sinkTo(cleanSink(config)).name("clean-user-events-sink");
        invalidEvents.sinkTo(invalidSink(config)).name("invalid-user-events-dlq-sink");

        env.execute("validation-enrichment-job");
    }

    private static KafkaSink<UserEvent> cleanSink(JobConfig config) {
        return KafkaSink.<UserEvent>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setRecordSerializer(
                        new KeyedJsonSerializationSchema<>(
                                config.cleanTopic(),
                                UserEvent::getUserId,
                                ValidationEnrichmentJob::userEventToJson
                        )
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }

    private static KafkaSink<DlqEvent> invalidSink(JobConfig config) {
        return KafkaSink.<DlqEvent>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setRecordSerializer(
                        new KeyedJsonSerializationSchema<>(
                                config.invalidDlqTopic(),
                                DlqEvent::getEventId,
                                ValidationEnrichmentJob::dlqEventToJson
                        )
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }

    private static String userEventToJson(UserEvent event) {
        try {
            return JsonSerdes.toJson(event);
        } catch (JsonProcessingException exc) {
            throw new IllegalArgumentException("Failed to serialize clean event", exc);
        }
    }

    private static String dlqEventToJson(DlqEvent event) {
        try {
            return JsonSerdes.toJson(event);
        } catch (JsonProcessingException exc) {
            throw new IllegalArgumentException("Failed to serialize DLQ event", exc);
        }
    }

    public static class ValidationMapFunction
            implements org.apache.flink.api.common.functions.MapFunction<String, ValidationResult> {
        private transient EventValidator validator;

        @Override
        public ValidationResult map(String rawJson) {
            if (validator == null) {
                validator = new EventValidator();
            }
            return validator.validate(rawJson);
        }
    }
}
