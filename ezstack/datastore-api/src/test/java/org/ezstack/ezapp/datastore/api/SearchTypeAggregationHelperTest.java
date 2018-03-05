package org.ezstack.ezapp.datastore.api;

import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

public class SearchTypeAggregationHelperTest {

    private SearchTypeAggregationHelper countHelper;
    private SearchTypeAggregationHelper maxHelper;
    private SearchTypeAggregationHelper minHelper;
    private SearchTypeAggregationHelper sumHelper;
    private SearchTypeAggregationHelper avgHelper;

    @Before
    public void buildHelper() {
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
    }

    @Test
    public void testComputeDocument() {
    }

}