package org.ezstack.ezapp.datastore.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Query {
    private final String _searchType;
    private final String _outerTable;
    private final Object _outerAttribute;
    private final String _innerTable;
    private final Object _innerAttribute;

    @JsonCreator
    public Query(String searchType,
                 String outerTable,
                 Object outerAttribute,
                 String innerTable,
                 Object innerAttribute) {
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
    public Object getOuterAttribute() {
        return _outerAttribute;
    }

    public DataType.JsonTypes getOuterAttributeType() {
        return DataType.getDataType(getOuterAttribute());
    }

    @JsonProperty("innerTable")
    public String getInnerTable() {
        return _innerTable;
    }

    @JsonProperty("innerAttribute")
    public Object getInnerAttribute() {
        return _innerAttribute;
    }

    public DataType.JsonTypes getInnerAttributeType() {
        return DataType.getDataType(getInnerAttribute());
    }
}
