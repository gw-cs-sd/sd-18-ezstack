package org.ezstack.ezapp.datastore.api;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class SearchTypeTest {

    private SearchType count;
    private SearchType max;
    private SearchType min;
    private SearchType sum;
    private SearchType avg;
    private SearchType search;
    private SearchType unknown;

    private SearchType count2;
    private SearchType count3;

    @Before
    public void buildSearchTypes() {
        count = new SearchType("count", "testAttribute");
        max = new SearchType("max", "testAttribute");
        min = new SearchType("min", "testAttribute");
        sum = new SearchType("sum", "testAttribute");
        avg = new SearchType("avg", "testAttribute");
        search = new SearchType("search", "testAttribute");
        unknown = new SearchType("blahblahblah", "testAttribute");

        count2 = new SearchType("count", "testAttribute");
        count3 = new SearchType("count", "notTestAttribute");
    }

    @Test
    public void testTypes() {
        assertEquals(count.getType(), SearchType.Type.COUNT);
        assertEquals(max.getType(), SearchType.Type.MAX);
        assertEquals(min.getType(), SearchType.Type.MIN);
        assertEquals(sum.getType(), SearchType.Type.SUM);
        assertEquals(avg.getType(), SearchType.Type.AVG);
        assertEquals(search.getType(), SearchType.Type.SEARCH);
        assertEquals(unknown.getType(), SearchType.Type.UNKNOWN);
    }

    @Test
    public void testTypeToString() {
        assertEquals(count.getType().toString(), SearchType.Type.COUNT.toString());
        assertEquals(max.getType().toString(), SearchType.Type.MAX.toString());
        assertEquals(min.getType().toString(), SearchType.Type.MIN.toString());
        assertEquals(sum.getType().toString(), SearchType.Type.SUM.toString());
        assertEquals(avg.getType().toString(), SearchType.Type.AVG.toString());
        assertEquals(search.getType().toString(), SearchType.Type.SEARCH.toString());
        assertEquals(unknown.getType().toString(), SearchType.Type.UNKNOWN.toString());
    }

    @Test
    public void testAttribute() {
        assertEquals(count.getAttributeOn(), "testAttribute");
        assertEquals(max.getAttributeOn(), "testAttribute");
        assertEquals(min.getAttributeOn(), "testAttribute");
        assertEquals(sum.getAttributeOn(), "testAttribute");
        assertEquals(avg.getAttributeOn(), "testAttribute");
        assertEquals(search.getAttributeOn(), "testAttribute");
        assertEquals(unknown.getAttributeOn(), "testAttribute");
    }

    @Test
    public void testAttributeToString() {
        assertEquals(count.toString(), "_count_testAttribute");
        assertEquals(max.toString(), "_max_testAttribute");
        assertEquals(min.toString(), "_min_testAttribute");
        assertEquals(sum.toString(), "_sum_testAttribute");
        assertEquals(avg.toString(), "_avg_testAttribute");
        assertEquals(search.toString(), "_search_testAttribute");
        assertEquals(unknown.toString(), "_unknown_testAttribute");
    }

    @Test
    public void testEquals() {
        assertTrue(count.equals(count2));
        assertFalse(count.equals(max));
        assertFalse(count.equals(count3));
    }

    @Test
    public void testHashcode() {
        assertEquals(count.hashCode(), count2.hashCode());
        assertNotEquals(count.hashCode(), count3.hashCode());
        assertNotEquals(count.hashCode(), max.hashCode());
    }

    @Test
    public void testHashcodeToString() {
        assertEquals(count.getMurmur3HashAsString(), count2.getMurmur3HashAsString());
        assertNotEquals(count.getMurmur3HashAsString(), count3.getMurmur3HashAsString());
        assertNotEquals(count.getMurmur3HashAsString(), max.getMurmur3HashAsString());
    }

}
