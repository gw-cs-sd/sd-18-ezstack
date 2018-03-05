package org.ezstack.ezapp.datastore.api;

import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class FilterTest {
    private static final int TEST_VALUE_1 = 12;
    private static final int TEST_VALUE_2 = 13;

    private static final String TEST_ATT_1 = "att1";
    private static final String TEST_ATT_2 = "hi";

    private static Filter f1, f2, f3, f4, f5, f6, f11, f8, f9;

    @BeforeClass
    public static void setup() {
        f1 = new Filter(TEST_ATT_1, "eq", TEST_VALUE_1);
        f2 = new Filter(TEST_ATT_1, "not_eq", TEST_VALUE_1);
        f3 = new Filter(TEST_ATT_1, "gte", TEST_VALUE_1);
        f4 = new Filter(TEST_ATT_1, "gt", TEST_VALUE_1);
        f5 = new Filter(TEST_ATT_1, "lte", TEST_VALUE_1);
        f6 = new Filter(TEST_ATT_1, "lt", TEST_VALUE_1);

        f11 = new Filter(TEST_ATT_1, "eq", TEST_VALUE_1);
        f8 = new Filter(TEST_ATT_2, "eq", TEST_VALUE_1);
        f9 = new Filter(TEST_ATT_1, "eq", TEST_VALUE_2);

    }

    @Test
    public void testEquals() {
        assertTrue(f1.equals(f11));
        assertFalse(f1.equals(f8));
        assertFalse(f1.equals(f9));
    }

    @Test
    public void testHashCode() {
        assertEquals(f1.hashCode(), f11.hashCode());
    }

    @Test
    public void testMurmurHash() {
        assertEquals(f1.getMurmur3HashAsString(), f11.getMurmur3HashAsString());
        assertNotEquals(f1.getMurmur3HashAsString(), f2.getMurmur3HashAsString());
    }

    @Test
    public void testGetValue() {
        assertEquals(f1.getValue(), TEST_VALUE_1);
        assertEquals(f9.getValue(), TEST_VALUE_2);
    }

    @Test
    public void testGetAttribute() {
        assertEquals(f1.getAttribute(), TEST_ATT_1);
        assertEquals(f8.getAttribute(), TEST_ATT_2);
    }

    @Test
    public void getOpt() {
        assertEquals(f1.getOp(), Filter.Operation.EQ);
        assertEquals(f2.getOp(), Filter.Operation.NOT_EQ);
        assertEquals(f3.getOp(), Filter.Operation.GTE);
        assertEquals(f4.getOp(), Filter.Operation.GT);
        assertEquals(f5.getOp(), Filter.Operation.LTE);
        assertEquals(f6.getOp(), Filter.Operation.LT);
    }
}