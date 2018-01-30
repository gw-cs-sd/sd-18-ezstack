package org.ezstack.ezapp.datastore.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;

public class QueryMetaData {
    private Query _query;
    private long _responseTimeInMs;

    @JsonCreator
    public QueryMetaData(@NotNull @JsonProperty("query") Query query,
                         @NotNull @JsonProperty("responseTimeInMs") long responseTimeInMs) {
        _query = query;
        _responseTimeInMs = responseTimeInMs;
    }

    @JsonProperty("query")
    public Query getQuery() {
        return _query;
    }

    @JsonProperty("responseTimeInMs")
    public long getResponseTimeInMs() {
        return _responseTimeInMs;
    }
}
