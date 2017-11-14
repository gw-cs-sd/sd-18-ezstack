package org.ezstack.ezapp.datastore.api;

import com.datastax.driver.core.utils.UUIDs;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class DocumentTest {

    private final String jsonDoc = "{\"_table\":\"comment\",\"_key\":\"dsfaf3\",\"_timestamp\":\"90cec5d0-c8b7-11e7-8a28-ff35eed18bcb\",\"_data\":{\"author\":{\"firstName\":\"Bob\",\"lastName\":\"Johnson\"},\"title\":\"Best Ever!\",\"likes\":50},\"_version\":2}";
    private final String jsonUpdate = "{\"_table\":\"comment\",\"_key\":\"dsfaf3\",\"_timestamp\":\"90cec5d0-c8b7-11e7-8a28-ff35eed18bcb\",\"_data\":{\"author\":{\"firstName\":\"Bob\",\"lastName\":\"Johnson\"},\"title\":\"Best Ever!\",\"likes\":50},\"_version\":2}";
    private Document document;
    private ObjectMapper mapper;

    @Before
    public void buildDoc() throws IOException {
        mapper = new ObjectMapper();
        document = mapper.readValue(jsonDoc, Document.class);
    }

    @Test
    public void testJsonConstructor() throws IOException {

        assertEquals("comment", document.getTable());
        assertEquals("dsfaf3", document.getKey());
        assertEquals(UUID.fromString("90cec5d0-c8b7-11e7-8a28-ff35eed18bcb"), document.getTimestamp());
        assertEquals(2, document.getVersion());
        assertEquals("{\"author\":{\"firstName\":\"Bob\",\"lastName\":\"Johnson\"},\"title\":\"Best Ever!\",\"likes\":50}",
                mapper.writeValueAsString(document.getData()));
    }

    @Test
    public void testUpdateConstructor() {
        UUID timestamp = UUIDs.timeBased();
        Map<String, Object> data = new HashMap<>();
        data.put("likes", 25);
        data.put("author", "Bob");

        Update update = new Update("posts", "abc123", timestamp, data, true);
        Document doc = new Document(update);

        assertEquals("posts", doc.getTable());
        assertEquals("abc123", doc.getKey());
        assertEquals(timestamp, doc.getTimestamp());
        assertEquals(1, doc.getVersion());
        assertEquals(data, doc.getData());
    }

    @Test
    public void testAddUpdateWithSmashUpdate() {
        UUID timestamp = UUIDs.timeBased();
        Map<String, Object> data = new HashMap<>();
        data.put("likes", 25);
        data.put("author", "Bob");

        Update update = new Update("comment", "dsfaf3", timestamp, data, false);
        document.addUpdate(update);

        assertEquals("comment", document.getTable());
        assertEquals("dsfaf3", document.getKey());
        assertEquals(timestamp, document.getTimestamp());
        assertEquals(3, document.getVersion());
        assertEquals(data, document.getData());
    }

    @Test
    public void testAddUpdateWithMerge() {
        UUID timestamp = UUIDs.timeBased();
        Map<String, Object> data = new HashMap<>();
        Map<String, Object> author = new HashMap<>();
        data.put("likes", 25);
        data.put("location", "New York");
        author.put("firstName", "Robert");
        author.put("email", "bob@yahoo.com");
        author.put("age", 22);
        data.put("author", author);

        Update update = new Update("comment", "dsfaf3", timestamp, data, true);
        document.addUpdate(update);

        Map<String, Object> expectedDataAfterMerge = new HashMap<>();
        Map<String, Object> expectedAuthorAfterMerge = new HashMap<>();

        expectedDataAfterMerge.put("likes", 25);
        expectedDataAfterMerge.put("title", "Best Ever!");
        expectedDataAfterMerge.put("location", "New York");
        expectedAuthorAfterMerge.put("firstName", "Robert");
        expectedAuthorAfterMerge.put("lastName", "Johnson");
        expectedAuthorAfterMerge.put("email", "bob@yahoo.com");
        expectedAuthorAfterMerge.put("age", 22);
        expectedDataAfterMerge.put("author", expectedAuthorAfterMerge);

        assertEquals("comment", document.getTable());
        assertEquals("dsfaf3", document.getKey());
        assertEquals(timestamp, document.getTimestamp());
        assertEquals(3, document.getVersion());
        assertEquals(expectedDataAfterMerge, document.getData());
    }

}
