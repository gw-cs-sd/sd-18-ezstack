package org.ezstack.ezapp.datastore.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.MoreObjects;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;

import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.MoreObjects.firstNonNull;

public class Query {
    @JsonIgnore
    private static final ObjectMapper mapper = new ObjectMapper();
    @JsonIgnore
    private static final String DEFAULT_JOIN_ATTRIBUTE_NAME = "_joinAttribute";

    private Set<SearchType> _searchTypes;
    private String _table;
    private Set<Filter> _filters;
    private Query _join;
    private String _joinAttributeName = DEFAULT_JOIN_ATTRIBUTE_NAME;
    private Set<JoinAttribute> _joinAttributes;

    private Set<String> _excludeAttributes;
    private Set<String> _includeAttributes;

    @JsonCreator
    public Query(@JsonProperty("searchTypes") Set<SearchType> searchTypes,
                 @JsonProperty("table") String table,
                 @JsonProperty("filter") Set<Filter> filters,
                 @JsonProperty("join") Query join,
                 @JsonProperty("joinAttributeName") String joinAttributeName,
                 @JsonProperty("joinAttributes") Set<JoinAttribute> joinAttributes,
                 @JsonProperty("excludeAttributes") Set<String> excludeAttributes,
                 @JsonProperty("includeAttributes") Set<String> includeAttributes) {

        checkNotNull(table, "table");

        _searchTypes = searchTypes;
        _table = table;
        _filters = filters;
        _join = join;
        _joinAttributeName = firstNonNull(joinAttributeName, DEFAULT_JOIN_ATTRIBUTE_NAME);
        _joinAttributes = joinAttributes;
        _excludeAttributes = excludeAttributes;
        _includeAttributes = includeAttributes;
    }

    @JsonProperty("searchTypes")
    public Set<SearchType> getSearchTypes() {
        return QueryHelper.safeSet(_searchTypes);
    }

    @JsonProperty("table")
    public String getTable() {
        return _table;
    }

    @JsonProperty("filter")
    public Set<Filter> getFilters() {
        return QueryHelper.safeSet(_filters);
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
    public Set<JoinAttribute> getJoinAttributes() {
        return QueryHelper.safeSet(_joinAttributes);
    }

    @JsonProperty("excludeAttributes")
    public Set<String> getExcludeAttributes() {
        return QueryHelper.safeSet(_excludeAttributes);
    }

    @JsonProperty("includeAttributes")
    public Set<String> getIncludeAttributes() {
        return QueryHelper.safeSet(_includeAttributes);
    }

    /**
     * new query includes the following data:
     * SearchTypes
     * Table
     * Join Query
     * JoinAttributes
     * JoinAttributeName is replaced with the default name
     * @return
     */
    @JsonIgnore
    public Query getStrippedQuery() {
        return new Query(_searchTypes, _table, null, _join != null ? _join.getStrippedQuery() : null,
                DEFAULT_JOIN_ATTRIBUTE_NAME, _joinAttributes, null, null);
    }

    /**
     * new query includes the following data:
     * SearchTypes
     * Table
     * Filters
     * Join Query
     * JoinAttributes
     * JoinAttributeName is replaced with the default name
     * @return
     */
    @JsonIgnore
    public Query getStrippedQueryWithFilters() {
        return new Query(_searchTypes, _table, _filters, _join != null ? _join.getStrippedQueryWithFilters() : null,
                DEFAULT_JOIN_ATTRIBUTE_NAME, _joinAttributes, null, null);
    }

    /**
     * new query includes the following data:
     * Table
     * Join Query
     * JoinAttributes
     * JoinAttributeName is replaced with the default name
     * @return
     */
    @JsonIgnore
    public Query getCoreQuery() {
        return new Query(null, _table, null, _join != null ? _join.getCoreQuery() : null,
                DEFAULT_JOIN_ATTRIBUTE_NAME, _joinAttributes, null, null);
    }

    public Query compactQuery(Query q) {
        return compactQuery(this, q);
    }

    @Override
    public String toString() {
        try {
            return mapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            return super.toString();
        }

    }

    @JsonIgnore
    public HashCode getMurmur3Hash() {
        StringBuilder sb = new StringBuilder();
        sb.append(getSearchTypes()).append("~");
        sb.append(_table).append("~");
        sb.append(getFilters()).append("~");
        if (_join != null) {
            sb.append(_join.getMurmur3HashAsString());
        }
        sb.append(getJoinAttributes()).append("~");
        sb.append(getExcludeAttributes()).append("~");
        sb.append(getIncludeAttributes()).append("~");
        return Hashing.murmur3_128().newHasher()
                .putString(sb.toString(), Charsets.UTF_8)
                .hash();
    }

    @JsonIgnore
    public String getMurmur3HashAsString() {
        return getMurmur3Hash().toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Query query = (Query) o;

        if (!getSearchTypes().equals(query.getSearchTypes())) return false;
        if (_table != null ? !_table.equals(query._table) : query._table != null) return false;
        if (_joinAttributeName != null ? !_joinAttributeName.equals(query._joinAttributeName) : query._joinAttributeName != null) return false;
        if (!getFilters().equals(query.getFilters())) return false;
        if (_join != null ? !_join.equals(query._join) : query._join != null) return false;
        if (!getJoinAttributes().equals(query.getJoinAttributes())) return false;
        if (!getExcludeAttributes().equals(query.getExcludeAttributes())) return false;
        return getIncludeAttributes().equals(query.getIncludeAttributes());
    }

    @Override
    public int hashCode() {
        int result = getSearchTypes().hashCode();
        result = 31 * result + (_table != null ? _table.hashCode() : 0);
        result = 31 * result + getFilters().hashCode();
        result = 31 * result + (_join != null ? _join.hashCode() : 0);
        result = 31 * result + (_joinAttributeName != null ? _joinAttributeName.hashCode() : 0);
        result = 31 * result + getJoinAttributes().hashCode();
        result = 31 * result + getExcludeAttributes().hashCode();
        result = 31 * result + getIncludeAttributes().hashCode();
        return result;
    }

    /**
     * If two queries share the same core query then they will be compacted into one large query that applies both scopes.
     * If they do not share the same core query it returns null.
     * @param q1
     * @param q2
     * @return
     */
    public static Query compactQuery(Query q1, Query q2) {
        if (!q1.getCoreQuery().equals(q2.getCoreQuery())) {
            return null; // queries don't share core query
        }

        Set<SearchType> sTypes1 = q1.getSearchTypes();
        Set<SearchType> sTypes2 = q2.getSearchTypes();
        Set<SearchType> sTypes = new HashSet<>(sTypes1);
        sTypes.addAll(sTypes2);

        Set<Filter> filters1 = q1.getFilters();
        Set<Filter> filters2 = q2.getFilters();
        Set<Filter> filters = new HashSet<>(filters1);
        filters.addAll(filters2);

        Set<JoinAttribute> joinAtt1 = q1.getJoinAttributes();
        Set<JoinAttribute> joinAtt2 = q2.getJoinAttributes();
        Set<JoinAttribute> joinAtt = new HashSet<>(joinAtt1);
        joinAtt.addAll(joinAtt2);

        Set<String> excludeAtt1 = q1.getExcludeAttributes();
        Set<String> excludeAtt2 = q2.getExcludeAttributes();
        Set<String> excludeAtt = new HashSet<>(excludeAtt1);
        excludeAtt.addAll(excludeAtt2);

        Set<String> includeAtt1 = q1.getIncludeAttributes();
        Set<String> includeAtt2 = q2.getIncludeAttributes();
        Set<String> includeAtt = new HashSet<>(includeAtt1);
        includeAtt.addAll(includeAtt2);

        Set<SearchType> searchTypes = sTypes.isEmpty() ? null : sTypes;
        Set<Filter> filtersList = filters.isEmpty() ? null : filters;
        Set<JoinAttribute> joinAttributes = joinAtt.isEmpty() ? null : joinAtt;
        Set<String> excludeAttributes = excludeAtt.isEmpty() ? null : excludeAtt;
        Set<String> includeAttributes = includeAtt.isEmpty() ? null : includeAtt;

        return new Query(searchTypes, q1.getTable(), filtersList,
                q1.getJoin() != null ? compactQuery(q1.getJoin(), q2.getJoin()) : null,
                DEFAULT_JOIN_ATTRIBUTE_NAME, joinAttributes, excludeAttributes, includeAttributes);
    }
}
