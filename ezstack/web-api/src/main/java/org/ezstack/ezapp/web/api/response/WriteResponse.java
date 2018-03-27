package org.ezstack.ezapp.web.api.response;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.Preconditions.checkNotNull;

public class WriteResponse {

    private String _key;

    @JsonCreator
    public WriteResponse(@JsonProperty("key") String key) {
        checkNotNull(key);
        _key = key;
    }

    @JsonProperty("key")
    public String getKey() {
        return _key;
    }
}

