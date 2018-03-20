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
        Set<SearchType> searchTypes = new HashSet<>();
        SearchType type = new SearchType("max", "testAttribute");
        searchTypes.add(type);

        Set<Filter> filters = new HashSet<>();
        Filter filter = new Filter("testAttribute", "eq", 5);
        filters.add(filter);

        Set<JoinAttribute> joinAttributes = new HashSet<>();
        JoinAttribute joinAttribute = new JoinAttribute("outer", "inner");
        joinAttributes.add(joinAttribute);

        Set<String> excludeAttributes = new HashSet<>();
        excludeAttributes.add("attribute1");

        Set<String> includeAttributes = new HashSet<>();
        includeAttributes.add("attribute2");

        query = new Query(searchTypes, "mytable", filters, null, "attribute", joinAttributes, excludeAttributes, includeAttributes);
    }

    @Test
    public void testGetters() {

        List<SearchType> searchlist = new ArrayList<>();
        SearchType type = new SearchType("max", "testAttribute");
        searchlist.add(type);
        Set<SearchType> searchSet = new HashSet<>(searchlist);
        assertEquals(query.getSearchTypes(), searchSet);

        List<Filter> filters = new ArrayList<>();
        Filter filter = new Filter("testAttribute", "eq", 5);
        filters.add(filter);
        Set<Filter> filterSet = new HashSet<>(filters);
        assertEquals(query.getFilters(), filterSet);

        List<JoinAttribute> joinAttributes = new ArrayList<>();
        JoinAttribute joinAttribute = new JoinAttribute("outer", "inner");
        joinAttributes.add(joinAttribute);
        Set<JoinAttribute> joinAttributeSet = new HashSet<>(joinAttributes);
        assertEquals(query.getJoinAttributes(), joinAttributeSet);

        List<String> excludeAttributes = new ArrayList<>();
        excludeAttributes.add("attribute1");
        Set<String> excludeAttributesSet = new HashSet<>(excludeAttributes);
        assertEquals(query.getExcludeAttributes(), excludeAttributesSet);

        List<String> includeAttributes = new ArrayList<>();
        includeAttributes.add("attribute2");
        Set<String> includeAttributesSet = new HashSet<>(includeAttributes);
        assertEquals(query.getIncludeAttributes(), includeAttributesSet);
    }

    @Test
    public void testCompactQuery() {
        Set<SearchType> searchTypes = new HashSet<>();
        SearchType type = new SearchType("max", "testAttribute");
        searchTypes.add(type);

        Set<Filter> filters = new HashSet<>();
        Filter filter = new Filter("testAttribute", "eq", 5);
        filters.add(filter);

        Set<JoinAttribute> joinAttributes = new HashSet<>();
        JoinAttribute joinAttribute = new JoinAttribute("outer", "inner");
        joinAttributes.add(joinAttribute);

        Set<String> excludeAttributes = new HashSet<>();
        excludeAttributes.add("attribute1");

        Set<String> includeAttributes = new HashSet<>();
        includeAttributes.add("attribute2");

        Query query2 = new Query(searchTypes, "mytable", filters, null, "attribute", joinAttributes, excludeAttributes, includeAttributes);
        Query query3 = new Query(searchTypes, "mytable2", filters, null, "attribute", joinAttributes, excludeAttributes, includeAttributes);

        assertEquals(query, query2);
        assertEquals(query.compactQuery(query2), query2.compactQuery(query));
        assertEquals(query.compactQuery(query3), null);
    }

    @Test
    public void testHash() {
        Set<SearchType> searchTypes = new HashSet<>();
        SearchType type = new SearchType("max", "testAttribute");
        searchTypes.add(type);

        Set<Filter> filters = new HashSet<>();
        Filter filter = new Filter("testAttribute", "eq", 5);
        filters.add(filter);

        Set<JoinAttribute> joinAttributes = new HashSet<>();
        JoinAttribute joinAttribute = new JoinAttribute("outer", "inner");
        joinAttributes.add(joinAttribute);

        Set<String> excludeAttributes = new HashSet<>();
        excludeAttributes.add("attribute1");

        Set<String> includeAttributes = new HashSet<>();
        includeAttributes.add("attribute2");

        Query query2 = new Query(searchTypes, "mytable", filters, null, "attribute", joinAttributes, excludeAttributes, includeAttributes);

        assertEquals(query.getMurmur3HashAsString(), query2.getMurmur3HashAsString());
        assertEquals(query.hashCode(), query2.hashCode());
    }

    @Test
    public void testToString() {
        Set<SearchType> searchTypes = new HashSet<>();
        SearchType type = new SearchType("max", "testAttribute");
        searchTypes.add(type);

        Set<Filter> filters = new HashSet<>();
        Filter filter = new Filter("testAttribute", "eq", 5);
        filters.add(filter);

        Set<JoinAttribute> joinAttributes = new HashSet<>();
        JoinAttribute joinAttribute = new JoinAttribute("outer", "inner");
        joinAttributes.add(joinAttribute);

        Set<String> excludeAttributes = new HashSet<>();
        excludeAttributes.add("attribute1");

        Set<String> includeAttributes = new HashSet<>();
        includeAttributes.add("attribute2");

        Query query2 = new Query(searchTypes, "mytable", filters, null, "attribute", joinAttributes, excludeAttributes, includeAttributes);

        assertEquals(query.toString(), query2.toString());
    }

    @Test
    public void testStrippedQuery() {
        Set<SearchType> searchTypes = new HashSet<>();
        SearchType type = new SearchType("max", "testAttribute");
        searchTypes.add(type);

        Set<Filter> filters = new HashSet<>();
        Filter filter = new Filter("testAttribute", "eq", 5);
        filters.add(filter);

        Set<JoinAttribute> joinAttributes = new HashSet<>();
        JoinAttribute joinAttribute = new JoinAttribute("outer", "inner");
        joinAttributes.add(joinAttribute);

        Set<String> excludeAttributes = new HashSet<>();
        excludeAttributes.add("attribute1");

        Set<String> includeAttributes = new HashSet<>();
        includeAttributes.add("attribute2");

        Query query2 = new Query(searchTypes, "mytable", filters, null, "attribute", joinAttributes, excludeAttributes, includeAttributes);

        assertEquals(query.getStrippedQuery(), query2.getStrippedQuery());
        assertEquals(query.getStrippedQueryWithFilters(), query2.getStrippedQueryWithFilters());
    }
}