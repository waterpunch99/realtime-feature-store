package com.example.featurestore.validation;

import com.example.featurestore.model.DlqEvent;
import com.example.featurestore.model.UserEvent;

public class ValidationResult {
    private final UserEvent event;
    private final DlqEvent dlqEvent;

    private ValidationResult(UserEvent event, DlqEvent dlqEvent) {
        this.event = event;
        this.dlqEvent = dlqEvent;
    }

    public static ValidationResult valid(UserEvent event) {
        return new ValidationResult(event, null);
    }

    public static ValidationResult invalid(DlqEvent dlqEvent) {
        return new ValidationResult(null, dlqEvent);
    }

    public boolean isValid() {
        return event != null;
    }

    public UserEvent getEvent() {
        return event;
    }

    public DlqEvent getDlqEvent() {
        return dlqEvent;
    }
}

