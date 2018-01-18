package org.ezstack.ezapp.datastore.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Query {
    private final String _searchType;
    private final String _outerTable;
    private final String _outerAttribute;
    private final String _innerTable;
    private final String _innerAttribute;

    @JsonCreator
    public Query(@JsonProperty("searchType") String searchType,
                 @JsonProperty("outerTable") String outerTable,
                 @JsonProperty("outerAttribute") String outerAttribute,
                 @JsonProperty("innerTable") String innerTable,
                 @JsonProperty("innerAttribute") String innerAttribute) {
        _searchType = searchType;
        _outerTable = outerTable;
        _outerAttribute = outerAttribute;
        _innerTable = innerTable;
        _innerAttribute = innerAttribute;
    }

    @JsonProperty("searchType")
    public String getSearchType() {
        return _searchType;
    }

    @JsonProperty("outerTable")
    public String getOuterTable() {
        return _outerTable;
    }

    @JsonProperty("outerAttribute")
    public String getOuterAttribute() {
        return _outerAttribute;
    }

    @JsonProperty("innerTable")
    public String getInnerTable() {
        return _innerTable;
    }

    @JsonProperty("innerAttribute")
    public String getInnerAttribute() {
        return _innerAttribute;
    }
}
