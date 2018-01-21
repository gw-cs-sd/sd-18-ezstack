package org.ezstack.ezapp.datastore.api;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class Query {
    @JsonProperty("searchType")
    private String searchType;

    @JsonProperty("table")
    private String table;

    @JsonProperty("filter")
    private List<Filter> filter;

    @JsonProperty("join")
    private Query join;

    @JsonProperty("matchAttribute")
    private List<MatchAttribute> matchAttribute;

    public String getSearchType() {
        return searchType;
    }

    public String getTable() {
        return table;
    }

    public List<Filter> getFilter() {
        return filter;
    }

    public Query getJoin() {
        return join;
    }

    public List<MatchAttribute> getMatchAttribute() {
        return matchAttribute;
    }
}
