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

public class IngestDelayLateEventRouter extends ProcessFunction<UserEvent, UserEvent> {
    public static final OutputTag<DlqEvent> LATE_EVENTS_TAG = new OutputTag<>("late-events-dlq") {
    };

    private final long allowedIngestDelaySeconds;

    public IngestDelayLateEventRouter(long allowedIngestDelaySeconds) {
        this.allowedIngestDelaySeconds = allowedIngestDelaySeconds;
    }

    @Override
    public void processElement(UserEvent event, Context context, Collector<UserEvent> out)
            throws Exception {
        if (event.getEventTime() == null || event.getIngestTime() == null) {
            out.collect(event);
            return;
        }

        long ingestDelaySeconds = Duration.between(event.getEventTime(), event.getIngestTime())
                .toSeconds();
        if (ingestDelaySeconds > allowedIngestDelaySeconds) {
            context.output(
                    LATE_EVENTS_TAG,
                    new DlqEvent(
                            event.getEventId(),
                            event.getEventType(),
                            event.getUserId(),
                            "event_time_late_beyond_ingest_delay",
                            "ingest_time - event_time is " + ingestDelaySeconds + " seconds",
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
