package org.ezstack.ezapp.datastore.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

public class QueryResult {
    private Object _count;
    private Object _max;
    private Object _min;
    private Object _sum;
    private Object _avg;
    private List<Map<String, Object>> _documents;

    public QueryResult(List<Map<String, Object>> documents) {
        _documents = documents;
    }

    @JsonCreator
    public QueryResult(@JsonProperty("_documents") List<Map<String, Object>> documents,
                       @JsonProperty("_count") Object count,
                       @JsonProperty("_max") Object max,
                       @JsonProperty("_min") Object min,
                       @JsonProperty("_sum") Object sum,
                       @JsonProperty("_avg") Object avg) {
        _documents = documents;
        _count = count;
        _max = max;
        _min = min;
        _sum = sum;
        _avg = avg;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty("_count")
    public Object getCount() {
        return isValidAggregation(_count);
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty("_max")
    public Object getMax() {
        return isValidAggregation(_max);
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty("_min")
    public Object getMin() {
        return isValidAggregation(_min);
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty("_sum")
    public Object getSum() {
        return isValidAggregation(_sum);
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty("_avg")
    public Object getAverage() {
        return isValidAggregation(_avg);
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty("_documents")
    public List<Map<String, Object>> getDocuments() {
        return _documents;
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
