package org.ezstack.ezapp.datastore.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

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

    private SearchTypeAggregationHelper countHelper2;
    private SearchTypeAggregationHelper maxHelper2;
    private SearchTypeAggregationHelper minHelper2;
    private SearchTypeAggregationHelper sumHelper2;
    private SearchTypeAggregationHelper avgHelper2;

    @Before
    public void buildHelper() throws IOException {
        SearchType count = new SearchType("count", "~version");
        countHelper = new SearchTypeAggregationHelper(count);
        SearchType max = new SearchType("max", "~version");
        maxHelper = new SearchTypeAggregationHelper(max);
        SearchType min = new SearchType("min", "~version");
        minHelper = new SearchTypeAggregationHelper(min);
        SearchType sum = new SearchType("sum", "~version");
        sumHelper = new SearchTypeAggregationHelper(sum);
        SearchType avg = new SearchType("avg", "~version");
        avgHelper = new SearchTypeAggregationHelper(avg);

        SearchType count2 = new SearchType("count", "~key");
        countHelper2 = new SearchTypeAggregationHelper(count2);
        SearchType max2 = new SearchType("max", "~key");
        maxHelper2 = new SearchTypeAggregationHelper(max2);
        SearchType min2 = new SearchType("min", "~key");
        minHelper2 = new SearchTypeAggregationHelper(min2);
        SearchType sum2 = new SearchType("sum", "~key");
        sumHelper2 = new SearchTypeAggregationHelper(sum2);
        SearchType avg2 = new SearchType("avg", "~key");
        avgHelper2 = new SearchTypeAggregationHelper(avg2);

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

        countHelper2.computeDocument(document);
        maxHelper2.computeDocument(document);
        minHelper2.computeDocument(document);
        sumHelper2.computeDocument(document);
        avgHelper2.computeDocument(document);

        assertEquals(countHelper.getResult(), (long)1);
        assertEquals(maxHelper.getResult(), (long)1);
        assertEquals(minHelper.getResult(), (long)1);
        assertEquals(sumHelper.getResult(), (long)1);
        assertEquals(avgHelper.getResult(), 1.0);

        assertEquals(countHelper2.getResult(), (long)1);
        assertEquals(maxHelper2.getResult(), 0);
        assertEquals(minHelper2.getResult(), 0);
        assertEquals(sumHelper2.getResult(), 0);
        assertEquals(avgHelper2.getResult(), 0);
    }
}