package org.ezstack.ezapp.datastore.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.validation.constraints.NotNull;
import java.util.List;

public class Query {
    @JsonIgnore
    private static final ObjectMapper mapper = new ObjectMapper();

    private List<SearchType> _searchType;
    private String _table;
    private List<Filter> _filters;
    private Query _join;
    private String _joinAttributeName = "_joinAttribute";
    private List<JoinAttribute> _joinAttributes;

    private List<String> _excludeAttributes;
    private List<String> _includeAttributes;

    @JsonCreator
    public Query(@JsonProperty("searchType") List<SearchType> searchType,
                 @NotNull @JsonProperty("table") String table,
                 @JsonProperty("filter") List<Filter> filters,
                 @JsonProperty("join") Query join,
                 @JsonProperty("joinAttributeName") String joinAttributeName,
                 @JsonProperty("joinAttributes") List<JoinAttribute> joinAttributes,
                 @JsonProperty("excludeAttributes") List<String> excludeAttributes,
                 @JsonProperty("includeAttributes") List<String> includeAttributes) {

        _searchType = searchType;
        _table = table;
        _filters = filters;
        _join = join;
        _joinAttributeName = joinAttributeName;
        _joinAttributes = joinAttributes;
        _excludeAttributes = excludeAttributes;
        _includeAttributes = includeAttributes;
    }

    @JsonProperty("searchType")
    public List<SearchType> getSearchType() {
        return _searchType;
    }

    @JsonProperty("table")
    public String getTable() {
        return _table;
    }

    @JsonProperty("filter")
    public List<Filter> getFilters() {
        return _filters;
    }

    @JsonProperty("join")
    public Query getJoin() {
        return _join;
    }

    @JsonProperty("joinAttributeName")
    public String getJoinAttributeName() {
        return _joinAttributeName;
    }

    @JsonProperty("joinAttributes")
    public List<JoinAttribute> getJoinAttributes() {
        return _joinAttributes;
    }

    @JsonProperty("excludeAttributes")
    public List<String> getExcludeAttributes() {
        return _excludeAttributes;
    }

    @JsonProperty("includeAttributes")
    public List<String> getIncludeAttributes() {
        return _includeAttributes;
    }

    public String toString() {
        try {
            return mapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            return super.toString();
        }

    }
}
