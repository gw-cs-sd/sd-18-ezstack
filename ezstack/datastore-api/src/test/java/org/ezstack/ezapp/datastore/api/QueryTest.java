package org.ezstack.ezapp.datastore.api;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class QueryTest {

    private Query query;

    @Before
    public void buildQuery() {
        List<SearchType> searchTypes = new ArrayList<>();
        SearchType type = new SearchType("max", "testAttribute");
        searchTypes.add(type);

        List<Filter> filters = new ArrayList<>();
        Filter filter = new Filter("testAttribute", "eq", 5);
        filters.add(filter);

        List<JoinAttribute> joinAttributes = new ArrayList<>();
        JoinAttribute joinAttribute = new JoinAttribute("outer", "inner");
        joinAttributes.add(joinAttribute);

        List<String> excludeAttributes = new ArrayList<>();
        excludeAttributes.add("attribute1");

        List<String> includeAttributes = new ArrayList<>();
        includeAttributes.add("attribute2");

        query = new Query(searchTypes, "myTable", filters, null, "attribute", joinAttributes, excludeAttributes, includeAttributes);
    }

    @Test
    public void testGetters() {

        List<SearchType> searchlist = new ArrayList<>();
        SearchType type = new SearchType("max", "testAttribute");
        searchlist.add(type);
        assertEquals(query.getSearchTypes(), searchlist);

        assertEquals(query.getTable(), "myTable");

        List<Filter> filters = new ArrayList<>();
        Filter filter = new Filter("testAttribute", "eq", 5);
        filters.add(filter);
        assertEquals(query.getFilters(), filters);

        assertEquals(query.getJoin(), null);

        assertEquals(query.getJoinAttributeName(), "attribute");

        List<JoinAttribute> joinAttributes = new ArrayList<>();
        JoinAttribute joinAttribute = new JoinAttribute("outer", "inner");
        joinAttributes.add(joinAttribute);
        assertEquals(query.getJoinAttributes(), joinAttributes);

        List<String> excludeAttributes = new ArrayList<>();
        excludeAttributes.add("attribute1");
        assertEquals(query.getExcludeAttributes(), excludeAttributes);

        List<String> includeAttributes = new ArrayList<>();
        includeAttributes.add("attribute2");
        assertEquals(query.getIncludeAttributes(), includeAttributes);
    }

    @Test
    public void testAsSet() {

        List<SearchType> searchlist = new ArrayList<>();
        SearchType type = new SearchType("max", "testAttribute");
        searchlist.add(type);
        Set<SearchType> searchSet = new HashSet<>(searchlist);
        assertEquals(query.getSearchTypesAsSet(), searchSet);

        List<Filter> filters = new ArrayList<>();
        Filter filter = new Filter("testAttribute", "eq", 5);
        filters.add(filter);
        Set<Filter> filterSet = new HashSet<>(filters);
        assertEquals(query.getFiltersAsSet(), filterSet);

        List<JoinAttribute> joinAttributes = new ArrayList<>();
        JoinAttribute joinAttribute = new JoinAttribute("outer", "inner");
        joinAttributes.add(joinAttribute);
        Set<JoinAttribute> joinAttributeSet = new HashSet<>(joinAttributes);
        assertEquals(query.getJoinAttributesAsSet(), joinAttributeSet);

        List<String> excludeAttributes = new ArrayList<>();
        excludeAttributes.add("attribute1");
        Set<String> excludeAttributesSet = new HashSet<>(excludeAttributes);
        assertEquals(query.getExcludeAttributesAsSet(), excludeAttributesSet);

        List<String> includeAttributes = new ArrayList<>();
        includeAttributes.add("attribute2");
        Set<String> includeAttributesSet = new HashSet<>(includeAttributes);
        assertEquals(query.getIncludeAttributesAsSet(), includeAttributesSet);
    }

    @Test
    public void testCompactQuery() {
        List<SearchType> searchTypes = new ArrayList<>();
        SearchType type = new SearchType("max", "testAttribute");
        searchTypes.add(type);

        List<Filter> filters = new ArrayList<>();
        Filter filter = new Filter("testAttribute", "eq", 5);
        filters.add(filter);

        List<JoinAttribute> joinAttributes = new ArrayList<>();
        JoinAttribute joinAttribute = new JoinAttribute("outer", "inner");
        joinAttributes.add(joinAttribute);

        List<String> excludeAttributes = new ArrayList<>();
        excludeAttributes.add("attribute1");

        List<String> includeAttributes = new ArrayList<>();
        includeAttributes.add("attribute2");

        Query query2 = new Query(searchTypes, "myTable", filters, null, "attribute", joinAttributes, excludeAttributes, includeAttributes);
        Query query3 = new Query(searchTypes, "myTable2", filters, null, "attribute", joinAttributes, excludeAttributes, includeAttributes);

        assertEquals(query, query2);
        assertEquals(query.compactQuery(query2), query);
        assertEquals(query.compactQuery(query2), query2);
        assertEquals(query2.compactQuery(query), query);
        assertEquals(query2.compactQuery(query), query2);
        assertEquals(query.compactQuery(query3), null);
    }

    @Test
    public void testHash() {
        List<SearchType> searchTypes = new ArrayList<>();
        SearchType type = new SearchType("max", "testAttribute");
        searchTypes.add(type);

        List<Filter> filters = new ArrayList<>();
        Filter filter = new Filter("testAttribute", "eq", 5);
        filters.add(filter);

        List<JoinAttribute> joinAttributes = new ArrayList<>();
        JoinAttribute joinAttribute = new JoinAttribute("outer", "inner");
        joinAttributes.add(joinAttribute);

        List<String> excludeAttributes = new ArrayList<>();
        excludeAttributes.add("attribute1");

        List<String> includeAttributes = new ArrayList<>();
        includeAttributes.add("attribute2");

        Query query2 = new Query(searchTypes, "myTable", filters, null, "attribute", joinAttributes, excludeAttributes, includeAttributes);

        assertEquals(query.getMurmur3HashAsString(), query2.getMurmur3HashAsString());
        assertEquals(query.hashCode(), query2.hashCode());
    }

    @Test
    public void testToString() {
        List<SearchType> searchTypes = new ArrayList<>();
        SearchType type = new SearchType("max", "testAttribute");
        searchTypes.add(type);

        List<Filter> filters = new ArrayList<>();
        Filter filter = new Filter("testAttribute", "eq", 5);
        filters.add(filter);

        List<JoinAttribute> joinAttributes = new ArrayList<>();
        JoinAttribute joinAttribute = new JoinAttribute("outer", "inner");
        joinAttributes.add(joinAttribute);

        List<String> excludeAttributes = new ArrayList<>();
        excludeAttributes.add("attribute1");

        List<String> includeAttributes = new ArrayList<>();
        includeAttributes.add("attribute2");

        Query query2 = new Query(searchTypes, "myTable", filters, null, "attribute", joinAttributes, excludeAttributes, includeAttributes);

        assertEquals(query.toString(), query2.toString());
    }

    @Test
    public void testStrippedQuery() {
        List<SearchType> searchTypes = new ArrayList<>();
        SearchType type = new SearchType("max", "testAttribute");
        searchTypes.add(type);

        List<Filter> filters = new ArrayList<>();
        Filter filter = new Filter("testAttribute", "eq", 5);
        filters.add(filter);

        List<JoinAttribute> joinAttributes = new ArrayList<>();
        JoinAttribute joinAttribute = new JoinAttribute("outer", "inner");
        joinAttributes.add(joinAttribute);

        List<String> excludeAttributes = new ArrayList<>();
        excludeAttributes.add("attribute1");

        List<String> includeAttributes = new ArrayList<>();
        includeAttributes.add("attribute2");

        Query query2 = new Query(searchTypes, "myTable", filters, null, "attribute", joinAttributes, excludeAttributes, includeAttributes);

        assertEquals(query.getStrippedQuery(), query2.getStrippedQuery());
        assertEquals(query.getStrippedQueryWithFilters(), query2.getStrippedQueryWithFilters());
    }
}