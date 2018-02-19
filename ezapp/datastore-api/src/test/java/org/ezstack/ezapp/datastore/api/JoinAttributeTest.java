package org.ezstack.ezapp.datastore.api;

import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class JoinAttributeTest {
    private static JoinAttribute ja1, ja11, ja2, ja22;

    @BeforeClass
    public static void setup() {
        ja1 = new JoinAttribute("att1", "att2");
        ja11 = new JoinAttribute("att1", "att2");

        ja2 = new JoinAttribute("att2", "att1");
        ja22 = new JoinAttribute("att2", "att1");
    }

    @Test
    public void testMurmurHash() {
        assertEquals(ja1.getMurmur3HashAsString(), ja11.getMurmur3HashAsString());
        assertEquals(ja2.getMurmur3HashAsString(), ja22.getMurmur3HashAsString());
        assertNotEquals(ja1.getMurmur3HashAsString(), ja2.getMurmur3HashAsString());
    }

    @Test
    public void testEquals() {
        assertTrue(ja1.equals(ja1));
        assertTrue(ja1.equals(ja11));
        assertTrue(ja2.equals(ja22));
        assertFalse(ja1.equals(ja2));
    }

    @Test
    public void testHashCode() {
        assertEquals(ja1.hashCode(), ja11.hashCode());
        assertEquals(ja2.hashCode(), ja22.hashCode());
    }
}
