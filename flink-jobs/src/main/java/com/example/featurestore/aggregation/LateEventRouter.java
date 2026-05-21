package com.example.featurestore.aggregation;

import com.example.featurestore.model.DlqEvent;
import com.example.featurestore.model.UserEvent;
import com.example.featurestore.serde.JsonSerdes;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.time.Duration;
import java.time.Instant;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class LateEventRouter extends ProcessFunction<UserEvent, UserEvent> {
    public static final OutputTag<DlqEvent> LATE_EVENTS_TAG = new OutputTag<>("late-events-dlq") {
    };

    private final long allowedLatenessSeconds;

    public LateEventRouter(long allowedLatenessSeconds) {
        this.allowedLatenessSeconds = allowedLatenessSeconds;
    }

    @Override
    public void processElement(UserEvent event, Context context, Collector<UserEvent> out)
            throws Exception {
        if (event.getEventTime() == null || event.getIngestTime() == null) {
            out.collect(event);
            return;
        }

        long latenessSeconds = Duration.between(event.getEventTime(), event.getIngestTime()).toSeconds();
        if (latenessSeconds > allowedLatenessSeconds) {
            context.output(
                    LATE_EVENTS_TAG,
                    new DlqEvent(
                            event.getEventId(),
                            event.getEventType(),
                            event.getUserId(),
                            "event_time_late_beyond_watermark",
                            "ingest_time - event_time is " + latenessSeconds + " seconds",
                            rawEvent(event),
                            Instant.now()
                    )
            );
            return;
        }

        out.collect(event);
    }

    private String rawEvent(UserEvent event) {
        try {
            return JsonSerdes.toJson(event);
        } catch (JsonProcessingException exc) {
            return "{}";
        }
    }
}

