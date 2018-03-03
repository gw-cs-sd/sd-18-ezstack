package org.ezstack.ezapp.web.resources;

import com.fasterxml.jackson.annotation.JsonProperty;

public class WriteResponse {

    private String _key;

    public WriteResponse(String key) {
        _key = key;
    }

    @JsonProperty("key")
    public String getKey() {
        return _key;
    }
}

