package org.ezstack.ezapp.datastore.api;

import java.util.Map;

public class Query {
    private Map<String, Object> _query;

    public Query(Map<String, Object> query) {
        _query = query;
    }

    public Map<String, Object> getQuery() {
        return _query;
    }

    // TODO
}
