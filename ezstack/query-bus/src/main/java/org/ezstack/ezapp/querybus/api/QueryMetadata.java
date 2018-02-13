package org.ezstack.ezapp.querybus.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.hash.Hashing;
import org.ezstack.ezapp.datastore.api.Query;

import javax.validation.constraints.NotNull;

public class QueryMetadata {
    private final String _queryIdentifier;
    private final Query _query;
    private final long _responseTimeInMs;

    @JsonCreator
    public QueryMetadata(@NotNull @JsonProperty("query") Query query,
                         @NotNull @JsonProperty("responseTimeInMs") long responseTimeInMs) {
        _query = query;
        _responseTimeInMs = responseTimeInMs;

        _queryIdentifier =  _query.getMurmur3HashAsString();
    }

    @JsonIgnore
    public String getQueryIdentifier() {
        return _queryIdentifier;
    }

    @JsonProperty("query")
    public Query getQuery() {
        return _query;
    }

    public String hash() {
        return _query.toString();
    }

    @JsonProperty("responseTimeInMs")
    public long getResponseTimeInMs() {
        return _responseTimeInMs;
    }
}
