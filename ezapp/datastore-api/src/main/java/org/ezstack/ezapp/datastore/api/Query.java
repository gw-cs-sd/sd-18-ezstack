package org.ezstack.ezapp.datastore.api;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class Query {
    @JsonProperty("searchType")
    private String _searchType;

    @JsonProperty("table")
    private String _table;

    @JsonProperty("filter")
    private List<Filter> _filters;

    @JsonProperty("join")
    private Query _join;

    @JsonProperty("joinAttribute")
    private String _joinAttribute = "joinedDocuments";

    @JsonProperty("matchAttribute")
    private List<MatchAttribute> _matchAttributes;

    /**
     * Empty constructor for serialization
     */
    public Query() {
    }

    public Query(String searchType, String table, List<Filter> filters, Query join, String joinAttribute, List<MatchAttribute> matchAttributes) {
        _searchType = searchType;
        _table = table;
        _filters = filters;
        _join = join;
        _joinAttribute = joinAttribute;
        _matchAttributes = matchAttributes;
    }

    public String getSearchType() {
        return _searchType;
    }

    public String getTable() {
        return _table;
    }

    public List<Filter> getFilter() {
        return _filters;
    }

    public Query getJoin() {
        return _join;
    }

    public String getJoinAttribute() {
        return _joinAttribute;
    }

    public List<MatchAttribute> getMatchAttribute() {
        return _matchAttributes;
    }
}
