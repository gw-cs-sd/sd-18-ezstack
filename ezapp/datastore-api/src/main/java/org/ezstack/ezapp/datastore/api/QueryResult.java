package org.ezstack.ezapp.datastore.api;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonUnwrapped;

import java.util.HashMap;
import java.util.Map;
import java.util.List;

public class QueryResult {
    private Map<String, Object> _queryResults;

    public QueryResult() {
        _queryResults = new HashMap<>();
    }

    @JsonAnyGetter
    public Map<String, Object> getQueryResults() {
        return _queryResults;
    }

    @JsonIgnore
    public boolean addAggregation(SearchType type, Object value) {
        if (SearchTypeAggregationHelper.isValidAggregation(value) == null || type.getType() == SearchType.Type.SEARCH) {
            return false;
        }

        _queryResults.put(type.toString(), value);
        return true;
    }

    @JsonIgnore
    public boolean addAggregation(SearchTypeAggregationHelper searchTypeAggregationHelper) {
        if (searchTypeAggregationHelper.getSearchType().getType() == SearchType.Type.SEARCH) {
            return false;
        }

        _queryResults.put(searchTypeAggregationHelper.getSearchType().toString(), searchTypeAggregationHelper.getResult());
        return true;
    }

    public void addAggregations(List<SearchTypeAggregationHelper> helpers) {
        helpers = QueryHelper.safe(helpers);
        for (SearchTypeAggregationHelper helper: helpers) {
            _queryResults.put(helper.getSearchType().toString(), helper.getResult());
        }
    }

    public void addDocuments(List<Map<String, Object>> docs) {
        _queryResults.put("_documents", docs);
    }


}
