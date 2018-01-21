package org.ezstack.ezapp.datastore.api;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class Query {
    @JsonProperty("searchType")
    private String _searchType;

    @JsonProperty("table")
    private String _table;

    @JsonProperty("filter")
    private List<Filter> _filter;

    @JsonProperty("join")
    private Query _join;

    @JsonProperty("matchAttribute")
    private List<MatchAttribute> _matchAttribute;

    public String getSearchType() {
        return _searchType;
    }

    public String getTable() {
        return _table;
    }

    public List<Filter> getFilter() {
        return _filter;
    }

    public Query getJoin() {
        return _join;
    }

    public List<MatchAttribute> getMatchAttribute() {
        return _matchAttribute;
    }
}
