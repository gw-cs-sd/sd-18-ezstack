package org.ezstack.ezapp.datastore.api;

public class QueryWeight {
    // maybe change the weights
    public static int MAX_SEARCH_TYPES_CLOSENESS = 10;
    public static int MAX_TABLE_NAME_CLOSENESS = 0; // must match for any type of queries to be considered close, hence default 0
    public static int MAX_FILTERS_CLOSENESS = 10;
    public static int MAX_JOIN_QUERY_CLOSENESS = 20;
    public static int MAX_JOIN_ATTRIBUTE_NAME_CLOSENESS = 0; // simple name swap, hence default 0
    public static int MAX_JOIN_ATTRIBUTES_CLOSENESS = 10;
    public static int MAX_EXCLUDE_ATTRIBUTES_CLOSENESS = 10;
    public static int MAX_INCLUDE_ATTRIBUTES_CLOSENESS = 10;
    public static int MAX_NORMALIZED_QUERY_CLOSENESS = MAX_SEARCH_TYPES_CLOSENESS + MAX_TABLE_NAME_CLOSENESS +
            MAX_FILTERS_CLOSENESS + MAX_JOIN_QUERY_CLOSENESS + MAX_JOIN_ATTRIBUTE_NAME_CLOSENESS +
            MAX_JOIN_ATTRIBUTES_CLOSENESS + MAX_EXCLUDE_ATTRIBUTES_CLOSENESS + MAX_INCLUDE_ATTRIBUTES_CLOSENESS;

    private int _searchTypes;
    private int _tableName;
    private int _filters;
    private int _joinQuery;
    private int _joinAttributeName;
    private int _joinAttributes;
    private int _excludeAttributes;
    private int _includeAttributes;

    public QueryWeight() {
        _searchTypes = -1;
        _tableName = -1;
        _filters = -1;
        _joinQuery = -1;
        _joinAttributeName = -1;
        _joinAttributes = -1;
        _excludeAttributes = -1;
        _includeAttributes = -1;
    }

    public QueryWeight setSearchTypes(int weight) {
        _searchTypes = weight;
        return this;
    }

    public int getSearchTypes() {
        return get(_searchTypes, MAX_SEARCH_TYPES_CLOSENESS);
    }

    public QueryWeight setTableName(int weight) {
        _tableName = weight;
        return this;
    }

    public int getTableName() {
        return get(_tableName, MAX_TABLE_NAME_CLOSENESS);
    }

    public QueryWeight setFilters(int weight) {
        _filters = weight;
        return this;
    }

    public int getFilters() {
        return get(_filters, MAX_FILTERS_CLOSENESS);
    }

    public QueryWeight setJoinQuery(int weight) {
        _joinQuery = weight;
        return this;
    }

    public int getJoinQuery() {
        return get(_joinQuery, MAX_JOIN_QUERY_CLOSENESS);
    }

    public QueryWeight setJoinAttributeName(int weight) {
        _joinAttributeName = weight;
        return this;
    }

    public int getJoinAttributeName() {
        return get(_joinAttributeName, MAX_JOIN_ATTRIBUTE_NAME_CLOSENESS);
    }

    public QueryWeight setJoinAttributes(int weight) {
        _joinAttributes = weight;
        return this;
    }

    public int getJoinAttributes() {
        return get(_joinAttributes, MAX_JOIN_ATTRIBUTES_CLOSENESS);
    }

    public QueryWeight setExcludeAttributes(int weight) {
        _excludeAttributes = weight;
        return this;
    }

    public int getExcludeAttributes() {
        return get(_excludeAttributes, MAX_EXCLUDE_ATTRIBUTES_CLOSENESS);
    }

    public QueryWeight setIncludeAttributes(int weight) {
        _includeAttributes = weight;
        return this;
    }

    public int getIncludeAttributes() {
        return get(_includeAttributes, MAX_INCLUDE_ATTRIBUTES_CLOSENESS);
    }

    public int getTotal() {
        return getSearchTypes() + getTableName() + getFilters() +
                getJoinQuery() + getJoinAttributeName() + getJoinAttributes() +
                getExcludeAttributes() + getIncludeAttributes();
    }

    private int get(int value, int defaultValue) {
        return value >= 0 ? value : defaultValue;
    }
}
