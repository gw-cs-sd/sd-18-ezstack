package org.ezstack.ezapp.datastore.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.*;

public class SearchTypeAggregationHelperTest {

    private final String jsonDoc = "{\"~table\":\"comment\",\"~key\":\"testAttribute\",\"~firstUpdateAt\":\"2017-11-13T21:13:59.213Z\",\"~lastUpdateAt\":\"2017-11-13T21:13:59.213Z\",\"author\":{\"firstName\":\"Bob\",\"lastName\":\"Johnson\"},\"title\":\"Best Ever!\",\"likes\":50,\"~version\":1}";
    private Document document;
    private ObjectMapper mapper;

    private SearchTypeAggregationHelper countHelper;
    private SearchTypeAggregationHelper maxHelper;
    private SearchTypeAggregationHelper minHelper;
    private SearchTypeAggregationHelper sumHelper;
    private SearchTypeAggregationHelper avgHelper;

    @Before
    public void buildHelper() throws IOException {
        SearchType count = new SearchType("count", "testAttribute");
        countHelper = new SearchTypeAggregationHelper(count);
        SearchType max = new SearchType("max", "testAttribute");
        maxHelper = new SearchTypeAggregationHelper(max);
        SearchType min = new SearchType("min", "testAttribute");
        minHelper = new SearchTypeAggregationHelper(min);
        SearchType sum = new SearchType("sum", "testAttribute");
        sumHelper = new SearchTypeAggregationHelper(sum);
        SearchType avg = new SearchType("avg", "testAttribute");
        avgHelper = new SearchTypeAggregationHelper(avg);

        mapper = new ObjectMapper();
        document = mapper.readValue(jsonDoc, Document.class);
    }

    @Test
    public void testComputeDocumentAndGetResult() {
        countHelper.computeDocument(document);
        maxHelper.computeDocument(document);
        minHelper.computeDocument(document);
        sumHelper.computeDocument(document);
        avgHelper.computeDocument(document);

        assertEquals(countHelper.getResult(), (long)0);
        assertEquals(maxHelper.getResult(), 0);
        assertEquals(minHelper.getResult(), 0);
        assertEquals(sumHelper.getResult(), 0);
        assertEquals(avgHelper.getResult(), 0);
    }


}