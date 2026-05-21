package com.example.featurestore.aggregation;

import com.example.featurestore.model.FeatureUpdate;
import java.time.Instant;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class ProductFeatureWindowFunction
        extends ProcessWindowFunction<FeatureAccumulator, FeatureUpdate, String, TimeWindow> {
    private final String windowSize;

    public ProductFeatureWindowFunction(String windowSize) {
        this.windowSize = windowSize;
    }

    @Override
    public void process(
            String key,
            Context context,
            Iterable<FeatureAccumulator> elements,
            Collector<FeatureUpdate> out
    ) {
        FeatureAccumulator acc = elements.iterator().next();
        out.collect(new FeatureUpdate(
                "product",
                key,
                windowSize,
                Instant.ofEpochMilli(context.window().getStart()),
                Instant.ofEpochMilli(context.window().getEnd()),
                FeatureMaps.productFeatures(acc, windowSize),
                Instant.now()
        ));
    }
}

