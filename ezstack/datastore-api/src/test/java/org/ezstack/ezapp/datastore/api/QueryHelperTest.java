package org.ezstack.ezapp.datastore.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

public class QueryHelperTest {

    private final String jsonDoc = "{\"~table\":\"comment\",\"~key\":\"dsfaf3\",\"~firstUpdateAt\":\"2017-11-13T21:13:59.213Z\",\"~lastUpdateAt\":\"2017-11-13T21:13:59.213Z\",\"author\":{\"firstName\":\"Bob\",\"lastName\":\"Johnson\"},\"title\":\"Best Ever!\",\"likes\":50,\"~version\":2}";
    private Document document;
    private ObjectMapper mapper;

    private Set<String> excludeAttributes;
    private Set<String> includeAttributes;
    private Set<SearchType> searchTypes;
    private Filter filter;
    private Set<Filter> filters;
    private SearchType type;

    private Set<SearchTypeAggregationHelper> helpers;

    @Before
    public void setUpHelper() throws IOException {
        mapper = new ObjectMapper();
        document = mapper.readValue(jsonDoc, Document.class);

        excludeAttributes = new HashSet<>();
        excludeAttributes.add("lastName");

        includeAttributes = new HashSet<>();
        includeAttributes.add("title");

        searchTypes = new HashSet<>();
        type = new SearchType("max", "likes");
        searchTypes.add(type);

        filters = new HashSet<>();
        filter = new Filter("firstName", "eq", "Bob");
        filters.add(filter);
    }

    @Test
    public void testfilterAttributes() {
        Document newDoc = QueryHelper.filterAttributes(excludeAttributes, includeAttributes, document);

        Map<String, Object> data = new HashMap<>();
        Map<String, Object> nested = new HashMap<>();
        nested.put("firstName", "Bob");
        nested.put("lastName", "Johnson");
        data.put("author", nested);
        data.put("title", "Best Ever!");
        data.put("likes", 50);

        assertEquals(data, document.getData());
    }

    @Test
    public void testCreateAggHelpers() {
        helpers = QueryHelper.createAggHelpers(searchTypes);

        Set<SearchTypeAggregationHelper> helperList = QueryHelper.createAggHelpers(searchTypes);

        assertEquals(helpers.contains(searchTypes), helperList.contains(searchTypes));
    }

    @Test
    public void testUpdateAggHelpers() {
        QueryHelper.updateAggHelpers(helpers, document);
    }

    @Test
    public void testHasSearchRequest() {
        assertFalse(QueryHelper.hasSearchRequest(searchTypes));
    }

    @Test
    public void testMeetFilters() {
        assertFalse(QueryHelper.meetsFilters(filters, document));
        assertFalse(QueryHelper.meetsFilter(filter, document));
    }
}
