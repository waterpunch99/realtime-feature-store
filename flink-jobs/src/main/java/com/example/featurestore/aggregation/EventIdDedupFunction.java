package com.example.featurestore.aggregation;

import com.example.featurestore.model.UserEvent;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class EventIdDedupFunction extends KeyedProcessFunction<String, UserEvent, UserEvent> {
    private final long ttlHours;
    private transient ValueState<Boolean> seenState;

    public EventIdDedupFunction(long ttlHours) {
        this.ttlHours = ttlHours;
    }

    @Override
    public void open(Configuration parameters) {
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.hours(ttlHours))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        ValueStateDescriptor<Boolean> descriptor =
                new ValueStateDescriptor<>("seen-event-id", Boolean.class);
        descriptor.enableTimeToLive(ttlConfig);
        seenState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(UserEvent event, Context context, Collector<UserEvent> out)
            throws Exception {
        Boolean seen = seenState.value();
        if (Boolean.TRUE.equals(seen)) {
            return;
        }
        seenState.update(Boolean.TRUE);
        out.collect(event);
    }
}

