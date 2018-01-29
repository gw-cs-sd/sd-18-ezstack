package org.ezstack.ezapp.datastore.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;
import java.util.List;

public class Query {

    private String _aggregationType;
    private String _aggregationAttributeName;
    private String _table;
    private List<Filter> _filters;
    private Query _join;
    private String _joinAttributeName = "_joinAttributeName";
    private List<JoinAttribute> _joinAttributes;

    @JsonCreator
    public Query(@JsonProperty("aggregationType") String aggregationType,
                 @JsonProperty("aggregationAttributeName") String aggregationAttributeName,
                 @NotNull @JsonProperty("table") String table,
                 @JsonProperty("filter") List<Filter> filters,
                 @JsonProperty("join") Query join,
                 @JsonProperty("joinAttributeName") String joinAttributeName,
                 @JsonProperty("joinAttributes") List<JoinAttribute> joinAttributes) {

        _aggregationType = aggregationType;
        _aggregationAttributeName = aggregationAttributeName;
        _table = table;
        _filters = filters;
        _join = join;
        _joinAttributeName = joinAttributeName;
        _joinAttributes = joinAttributes;
    }

    public String getAggregationType() {
        return _aggregationType;
    }

    public String getAggregationAttributeName() {
        return _aggregationAttributeName;
    }

    public String getTable() {
        return _table;
    }

    public List<Filter> getFilters() {
        return _filters;
    }

    public Query getJoin() {
        return _join;
    }

    public String getJoinAttributeName() {
        return _joinAttributeName;
    }

    public List<JoinAttribute> getJoinAttributes() {
        return _joinAttributes;
    }
}
