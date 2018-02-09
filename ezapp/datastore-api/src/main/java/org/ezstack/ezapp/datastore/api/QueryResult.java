package org.ezstack.ezapp.datastore.api;

import com.fasterxml.jackson.annotation.JsonUnwrapped;

import java.util.HashMap;
import java.util.Map;
import java.util.List;

public class QueryResult {
    private Map<String, Object> _queryResults;

    public QueryResult() {
        _queryResults = new HashMap<>();
    }

    @JsonUnwrapped
    public Map<String, Object> getQueryResults() {
        return _queryResults;
    }

    public boolean addAggregation(SearchType type, Object value) {
        if (SearchTypeAggregationHelper.isValidAggregation(value) == null) {
            return false;
        }

        _queryResults.put(type.toString(), value);
        return true;
    }

    public void addDocuments(List<Map<String, Object>> docs) {
        _queryResults.put("_documents", docs);
    }


}
