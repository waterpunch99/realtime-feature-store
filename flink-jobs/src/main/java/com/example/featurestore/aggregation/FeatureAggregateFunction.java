package com.example.featurestore.aggregation;

import com.example.featurestore.model.UserEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

public class FeatureAggregateFunction
        implements AggregateFunction<UserEvent, FeatureAccumulator, FeatureAccumulator> {
    @Override
    public FeatureAccumulator createAccumulator() {
        return new FeatureAccumulator();
    }

    @Override
    public FeatureAccumulator add(UserEvent value, FeatureAccumulator accumulator) {
        accumulator.add(value);
        return accumulator;
    }

    @Override
    public FeatureAccumulator getResult(FeatureAccumulator accumulator) {
        return accumulator;
    }

    @Override
    public FeatureAccumulator merge(FeatureAccumulator a, FeatureAccumulator b) {
        return a.merge(b);
    }
}

