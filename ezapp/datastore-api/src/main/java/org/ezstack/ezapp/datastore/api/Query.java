package org.ezstack.ezapp.datastore.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;

import javax.validation.constraints.NotNull;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Query {
    @JsonIgnore
    private static final ObjectMapper mapper = new ObjectMapper();

    private List<SearchType> _searchTypes;
    private String _table;
    private List<Filter> _filters;
    private Query _join;
    private String _joinAttributeName = "_joinAttribute";
    private List<JoinAttribute> _joinAttributes;

    private List<String> _excludeAttributes;
    private List<String> _includeAttributes;

    @JsonCreator
    public Query(@JsonProperty("searchTypes") List<SearchType> searchTypes,
                 @NotNull @JsonProperty("table") String table,
                 @JsonProperty("filter") List<Filter> filters,
                 @JsonProperty("join") Query join,
                 @JsonProperty("joinAttributeName") String joinAttributeName,
                 @JsonProperty("joinAttributes") List<JoinAttribute> joinAttributes,
                 @JsonProperty("excludeAttributes") List<String> excludeAttributes,
                 @JsonProperty("includeAttributes") List<String> includeAttributes) {

        _searchTypes = searchTypes;
        _table = table;
        _filters = filters;
        _join = join;
        _joinAttributeName = joinAttributeName;
        _joinAttributes = joinAttributes;
        _excludeAttributes = excludeAttributes;
        _includeAttributes = includeAttributes;
    }

    @JsonProperty("searchTypes")
    public List<SearchType> getSearchTypes() {
        return _searchTypes;
    }

    @JsonIgnore
    public Set<SearchType> getSearchTypesAsSet() {
        if (_searchTypes == null || _searchTypes.isEmpty()) {
            return Collections.emptySet();
        }

        return new HashSet<>(_searchTypes);
    }

    @JsonProperty("table")
    public String getTable() {
        return _table;
    }

    @JsonProperty("filter")
    public List<Filter> getFilters() {
        return _filters;
    }

    @JsonIgnore
    public Set<Filter> getFiltersAsSet() {
        if (_filters == null || _filters.isEmpty()) {
            return Collections.emptySet();
        }

        return new HashSet<>(_filters);
    }

    @JsonIgnore

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

    @JsonIgnore
    public Set<JoinAttribute> getJoinAttributesAsSet() {
        if (_joinAttributes == null || _joinAttributes.isEmpty()) {
            return Collections.emptySet();
        }

        return new HashSet<>(_joinAttributes);
    }

    @JsonProperty("excludeAttributes")
    public List<String> getExcludeAttributes() {
        return _excludeAttributes;
    }

    @JsonIgnore
    public Set<String> getExcludeAttributesAsSet() {
        if (_excludeAttributes == null || _excludeAttributes.isEmpty()) {
            return Collections.emptySet();
        }

        return new HashSet<>(_excludeAttributes);
    }

    @JsonProperty("includeAttributes")
    public List<String> getIncludeAttributes() {
        return _includeAttributes;
    }

    @JsonIgnore
    public Set<String> getIncludeAttributesAsSet() {
        if (_includeAttributes == null || _includeAttributes.isEmpty()) {
            return Collections.emptySet();
        }

        return new HashSet<>(_includeAttributes);
    }

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
        sb.append(getSearchTypesAsSet()).append("~");
        sb.append(_table).append("~");
        sb.append(getFiltersAsSet()).append("~");
        if (_join != null) {
            sb.append(_join.getMurmur3HashAsString());
        }
        sb.append(getJoinAttributesAsSet()).append("~");
        sb.append(getExcludeAttributesAsSet()).append("~");
        sb.append(getIncludeAttributesAsSet()).append("~");
        return Hashing.murmur3_128().newHasher()
                .putString(sb.toString(), Charsets.UTF_8)
                .hash();
    }

    public String getMurmur3HashAsString() {
        return getMurmur3Hash().toString();
    }

    /**
     * Returns True if the following set of items match:
     * Same SearchTypes (order does not matter)
     * Same Table Name
     * Same Filters (order does not matter)
     * Same join query if it exists
     * Same exclude attributes (order does not matter)
     * Same include attributes (order does not matter)
     *
     * --- note that we never compare joinAttributeName because it has no impact on the query result ---
     * @param o
     * @return
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Query query = (Query) o;

        if (!getSearchTypesAsSet().equals(query.getSearchTypesAsSet())) return false;
        if (_table != null ? !_table.equals(query._table) : query._table != null) return false;
        if (!getFiltersAsSet().equals(query.getFiltersAsSet())) return false;
        if (_join != null ? !_join.equals(query._join) : query._join != null) return false;
        if (!getJoinAttributesAsSet().equals(query.getJoinAttributesAsSet())) return false;
        if (!getExcludeAttributesAsSet().equals(query.getExcludeAttributesAsSet())) return false;
        return getIncludeAttributesAsSet().equals(query.getIncludeAttributesAsSet());
    }

    @Override
    public int hashCode() {
        int result = getSearchTypesAsSet().hashCode();
        result = 31 * result + (_table != null ? _table.hashCode() : 0);
        result = 31 * result + getFiltersAsSet().hashCode();
        result = 31 * result + (_join != null ? _join.hashCode() : 0);
        result = 31 * result + getJoinAttributesAsSet().hashCode();
        result = 31 * result + getExcludeAttributesAsSet().hashCode();
        result = 31 * result + getIncludeAttributesAsSet().hashCode();
        return result;
    }
}
