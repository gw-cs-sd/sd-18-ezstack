package org.ezstack.ezapp.datastore.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class QueryHelperTest {

    private final String jsonDoc = "{\"~table\":\"comment\",\"~key\":\"dsfaf3\",\"~firstUpdateAt\":\"2017-11-13T21:13:59.213Z\",\"~lastUpdateAt\":\"2017-11-13T21:13:59.213Z\",\"author\":{\"firstName\":\"Bob\",\"lastName\":\"Johnson\"},\"title\":\"Best Ever!\",\"likes\":50,\"~version\":2}";
    private Document document;
    private ObjectMapper mapper;

    private List<String> excludeAttributes;
    private List<String> includeAttributes;
    private List<SearchType> searchTypes;
    private Filter filter;
    private List<Filter> filters;
    private SearchType type;

    private List<SearchTypeAggregationHelper> helpers;

    @Before
    public void setUpHelper() throws IOException {
        mapper = new ObjectMapper();
        document = mapper.readValue(jsonDoc, Document.class);

        excludeAttributes = new ArrayList<>();
        excludeAttributes.add("lastName");

        includeAttributes = new ArrayList<>();
        includeAttributes.add("title");

        searchTypes = new ArrayList<>();
        type = new SearchType("max", "likes");
        searchTypes.add(type);

        filters = new ArrayList<>();
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

        List<SearchTypeAggregationHelper> helperList = QueryHelper.createAggHelpers(searchTypes);

        assertEquals(helpers.get(0).getSearchType(), helperList.get(0).getSearchType());
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
