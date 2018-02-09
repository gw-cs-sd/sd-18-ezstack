package org.ezstack.ezapp.datastore.api;

import com.fasterxml.jackson.annotation.JsonUnwrapped;

import java.util.Map;

public class QueryResult {
    private Map<String, Object> _queryResults;

    @JsonUnwrapped
    public Map<String, Object> getQueryResults() {
        return _queryResults;
    }

    public void setQueryResults( Map<String, Object> queryResults) {
        _queryResults = queryResults;
    }

    /**
     * @param agg
     * @return agg if it is of type int or double, otherwise null
     */
    public static Object isValidAggregation(Object agg) {
        switch (DataType.getDataType(agg)) {
            case DOUBLE:
            case INTEGER: // fall through
                return agg;
            default:
                return null;
        }
    }
}
