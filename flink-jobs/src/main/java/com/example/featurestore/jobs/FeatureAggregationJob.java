package com.example.featurestore.jobs;

import com.example.featurestore.aggregation.CategoryFeatureWindowFunction;
import com.example.featurestore.aggregation.EventIdDedupFunction;
import com.example.featurestore.aggregation.FeatureAggregateFunction;
import com.example.featurestore.aggregation.IngestDelayLateEventRouter;
import com.example.featurestore.aggregation.ProductFeatureWindowFunction;
import com.example.featurestore.aggregation.UserFeatureWindowFunction;
import com.example.featurestore.config.JobConfig;
import com.example.featurestore.kafka.KeyedJsonSerializationSchema;
import com.example.featurestore.model.DlqEvent;
import com.example.featurestore.model.FeatureUpdate;
import com.example.featurestore.model.UserEvent;
import com.example.featurestore.serde.JsonSerdes;
import com.example.featurestore.sink.PostgresFeatureSink;
import com.example.featurestore.sink.RedisFeatureSink;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

public class FeatureAggregationJob {
    private static final long ALLOWED_INGEST_DELAY_SECONDS = 120L;

    public static void main(String[] args) throws Exception {
        JobConfig config = JobConfig.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        configureStateAndCheckpointing(env, config);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setTopics(config.cleanTopic())
                .setGroupId(config.consumerGroupId())
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<UserEvent> cleanEvents = env.fromSource(
                        source,
                        WatermarkStrategy.noWatermarks(),
                        "clean-user-events-source"
                )
                .map(FeatureAggregationJob::parseUserEvent)
                .name("parse-clean-event-json")
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
                .name("event-time-watermark");

        DataStream<UserEvent> deduplicatedEvents = cleanEvents
                .keyBy(UserEvent::getEventId)
                .process(new EventIdDedupFunction(config.dedupTtlHours()))
                .name("event-id-dedup-with-ttl");

        SingleOutputStreamOperator<UserEvent> onTimeEvents = deduplicatedEvents
                .process(new IngestDelayLateEventRouter(ALLOWED_INGEST_DELAY_SECONDS))
                .name("ingest-delay-late-event-router");

        DataStream<DlqEvent> lateEvents =
                onTimeEvents.getSideOutput(IngestDelayLateEventRouter.LATE_EVENTS_TAG);
        lateEvents.sinkTo(lateSink(config)).name("late-events-dlq-sink");

        DataStream<FeatureUpdate> userFeatures = userFeatures(onTimeEvents);
        DataStream<FeatureUpdate> productFeatures = productFeatures(onTimeEvents);
        DataStream<FeatureUpdate> categoryFeatures = categoryFeatures(onTimeEvents);

        userFeatures.sinkTo(featureSink(config.bootstrapServers(), config.featureUserTopic()))
                .name("feature-user-updates-sink");
        productFeatures.sinkTo(featureSink(config.bootstrapServers(), config.featureProductTopic()))
                .name("feature-product-updates-sink");
        categoryFeatures.sinkTo(featureSink(config.bootstrapServers(), config.featureCategoryTopic()))
                .name("feature-category-updates-sink");

        userFeatures.addSink(redisSink(config)).name("redis-user-feature-sink");
        productFeatures.addSink(redisSink(config)).name("redis-product-feature-sink");
        categoryFeatures.addSink(redisSink(config)).name("redis-category-feature-sink");

        userFeatures.addSink(postgresSink(config)).name("postgres-user-feature-sink");
        productFeatures.addSink(postgresSink(config)).name("postgres-product-feature-sink");
        categoryFeatures.addSink(postgresSink(config)).name("postgres-category-feature-sink");

        env.execute("feature-aggregation-job");
    }

    private static void configureStateAndCheckpointing(
            StreamExecutionEnvironment env,
            JobConfig config
    ) {
        if ("hashmap".equalsIgnoreCase(config.stateBackend())) {
            env.setStateBackend(new HashMapStateBackend());
        } else {
            throw new IllegalArgumentException(
                    "Unsupported state.backend for STEP 8 MVP: " + config.stateBackend()
            );
        }
        env.enableCheckpointing(config.checkpointIntervalMillis(), CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage(config.checkpointPath());
    }

    private static DataStream<FeatureUpdate> userFeatures(DataStream<UserEvent> events) {
        DataStream<FeatureUpdate> user10m = events
                .keyBy(UserEvent::getUserId)
                .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(1)))
                .aggregate(new FeatureAggregateFunction(), new UserFeatureWindowFunction("10m"))
                .name("user-features-10m");

        DataStream<FeatureUpdate> user1h = events
                .keyBy(UserEvent::getUserId)
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .aggregate(new FeatureAggregateFunction(), new UserFeatureWindowFunction("1h"))
                .name("user-features-1h");

        return user10m.union(user1h);
    }

    private static DataStream<FeatureUpdate> productFeatures(DataStream<UserEvent> events) {
        DataStream<UserEvent> productEvents = events
                .filter(event -> event.getProductId() != null && !event.getProductId().isBlank())
                .name("filter-product-events");

        DataStream<FeatureUpdate> product10m = productEvents
                .keyBy(UserEvent::getProductId)
                .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(1)))
                .aggregate(new FeatureAggregateFunction(), new ProductFeatureWindowFunction("10m"))
                .name("product-features-10m");

        DataStream<FeatureUpdate> product1h = productEvents
                .keyBy(UserEvent::getProductId)
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .aggregate(new FeatureAggregateFunction(), new ProductFeatureWindowFunction("1h"))
                .name("product-features-1h");

        return product10m.union(product1h);
    }

    private static DataStream<FeatureUpdate> categoryFeatures(DataStream<UserEvent> events) {
        DataStream<UserEvent> categoryEvents = events
                .filter(event -> event.getCategoryId() != null && !event.getCategoryId().isBlank())
                .name("filter-category-events");

        DataStream<FeatureUpdate> category10m = categoryEvents
                .keyBy(UserEvent::getCategoryId)
                .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(1)))
                .aggregate(new FeatureAggregateFunction(), new CategoryFeatureWindowFunction("10m"))
                .name("category-features-10m");

        DataStream<FeatureUpdate> category1h = categoryEvents
                .keyBy(UserEvent::getCategoryId)
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .aggregate(new FeatureAggregateFunction(), new CategoryFeatureWindowFunction("1h"))
                .name("category-features-1h");

        return category10m.union(category1h);
    }

    private static KafkaSink<FeatureUpdate> featureSink(String bootstrapServers, String topic) {
        return KafkaSink.<FeatureUpdate>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(
                        new KeyedJsonSerializationSchema<>(
                                topic,
                                FeatureUpdate::getEntityId,
                                FeatureAggregationJob::featureUpdateToJson
                        )
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }

    private static KafkaSink<DlqEvent> lateSink(JobConfig config) {
        return KafkaSink.<DlqEvent>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setRecordSerializer(
                        new KeyedJsonSerializationSchema<>(
                                config.lateDlqTopic(),
                                DlqEvent::getEventId,
                                FeatureAggregationJob::dlqEventToJson
                        )
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }

    private static RedisFeatureSink redisSink(JobConfig config) {
        return new RedisFeatureSink(config.redisHost(), config.redisPort());
    }

    private static PostgresFeatureSink postgresSink(JobConfig config) {
        return new PostgresFeatureSink(config.jdbcUrl(), config.jdbcUser(), config.jdbcPassword());
    }

    private static UserEvent parseUserEvent(String rawJson) throws JsonProcessingException {
        return JsonSerdes.parseUserEvent(rawJson);
    }

    private static String featureUpdateToJson(FeatureUpdate update) {
        try {
            return JsonSerdes.toJson(update);
        } catch (JsonProcessingException exc) {
            throw new IllegalArgumentException("Failed to serialize feature update", exc);
        }
    }

    private static String dlqEventToJson(DlqEvent event) {
        try {
            return JsonSerdes.toJson(event);
        } catch (JsonProcessingException exc) {
            throw new IllegalArgumentException("Failed to serialize late DLQ event", exc);
        }
    }
}
