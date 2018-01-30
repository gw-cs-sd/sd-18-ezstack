package org.ezstack.ezapp.datastore.api;

public class QueryMetaData {
    private Query _query;
    private long _responseTimeInMs;

    public QueryMetaData(Query query, long responseTimeInMs) {
        _query = query;
        _responseTimeInMs = responseTimeInMs;
    }

    public Query getQuery() {
        return _query;
    }

    public long getResponseTimeInMs() {
        return _responseTimeInMs;
    }
}
