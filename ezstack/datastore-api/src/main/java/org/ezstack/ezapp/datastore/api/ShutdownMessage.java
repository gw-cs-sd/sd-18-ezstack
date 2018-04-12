package org.ezstack.ezapp.datastore.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ShutdownMessage {

    private static final ShutdownMessage INSTANCE = new ShutdownMessage(true);

    @JsonCreator
    private ShutdownMessage(@JsonProperty("shutdown") boolean shutdown) {
    }

    public static ShutdownMessage instance() {
        return INSTANCE;
    }

    public boolean isShutdown() {
        return true;
    }
}
