package org.ezstack.ezapp.datastore.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;
import java.util.List;

public class Query {

    private List<SearchType> _searchType;
    private String _table;
    private List<Filter> _filters;
    private Query _join;
    private String _joinAttributeName = "_joinAttribute";
    private List<JoinAttribute> _joinAttributes;
    private List<String> _excludeAttributes;

    @JsonCreator
    public Query(@JsonProperty("searchType") List<SearchType> searchType,
                 @NotNull @JsonProperty("table") String table,
                 @JsonProperty("filter") List<Filter> filters,
                 @JsonProperty("join") Query join,
                 @JsonProperty("joinAttribute") String joinAttributeName,
                 @JsonProperty("joinAttributes") List<JoinAttribute> joinAttributes,
                 @JsonProperty("excludeAttributes") List<String> excludeAttributes) {

         _searchType = searchType;
        _table = table;
        _filters = filters;
        _join = join;
        _joinAttributeName = joinAttributeName;
        _joinAttributes = joinAttributes;
        _excludeAttributes = excludeAttributes;
    }

    public List<SearchType> getSearchType() {
        return _searchType;
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

    public List<String> getExcludeAttributes() {
        return _excludeAttributes;
    }
}
