package com.example.featurestore.sink;

import com.example.featurestore.model.FeatureUpdate;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

public class RedisFeatureSink extends RichSinkFunction<FeatureUpdate> {
    private final String host;
    private final int port;
    private transient Jedis jedis;

    public RedisFeatureSink(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    public void open(Configuration parameters) {
        jedis = new Jedis(host, port);
    }

    @Override
    public void invoke(FeatureUpdate update, Context context) {
        String key = hashKey(update);
        Map<String, String> hashValues = new HashMap<>();
        update.getFeatures().forEach((featureName, value) ->
                hashValues.put(featureName, FeatureValueConverters.jsonValue(value))
        );
        hashValues.put("window_size", update.getWindowSize());
        hashValues.put("window_start", update.getWindowStart().toString());
        hashValues.put("window_end", update.getWindowEnd().toString());
        hashValues.put("updated_at", update.getUpdatedAt().toString());
        jedis.hset(key, hashValues);

        writeRanking(update);
    }

    @Override
    public void close() {
        if (jedis != null) {
            jedis.close();
        }
    }

    private String hashKey(FeatureUpdate update) {
        return switch (update.getEntityType()) {
            case "user" -> "feature:user:" + update.getEntityId();
            case "product" -> "feature:product:" + update.getEntityId();
            case "category" -> "feature:category:" + update.getEntityId();
            default -> throw new IllegalArgumentException("Unknown entity_type: " + update.getEntityType());
        };
    }

    private void writeRanking(FeatureUpdate update) {
        if (!"10m".equals(update.getWindowSize())) {
            return;
        }
        if ("product".equals(update.getEntityType())) {
            BigDecimal score = FeatureValueConverters.decimalValue(
                    update.getFeatures(),
                    "product_popularity_score_10m"
            );
            jedis.zadd("rank:product:popular:10m", score.doubleValue(), update.getEntityId());
        }
        if ("category".equals(update.getEntityType())) {
            BigDecimal score = FeatureValueConverters.decimalValue(
                    update.getFeatures(),
                    "category_popularity_score_10m"
            );
            jedis.zadd("rank:category:popular:10m", score.doubleValue(), update.getEntityId());
        }
    }
}
