package org.ezstack.ezapp.denormalizer.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Pointer {

    @JsonProperty
    private final String _table;

    @JsonProperty
    private final String _id;

    @JsonCreator
    public Pointer(@JsonProperty("_table") String table, @JsonProperty("_id") String id) {
        _table = table;
        _id = id;
    }
}
