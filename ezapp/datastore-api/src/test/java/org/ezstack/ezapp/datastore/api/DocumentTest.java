package org.ezstack.ezapp.datastore.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class DocumentTest {

    private final String jsonDoc = "{\"~table\":\"comment\",\"~key\":\"dsfaf3\",\"~firstUpdateAt\":\"2017-11-13T21:13:59.213Z\",\"~lastUpdateAt\":\"2017-11-13T21:13:59.213Z\",\"author\":{\"firstName\":\"Bob\",\"lastName\":\"Johnson\"},\"title\":\"Best Ever!\",\"likes\":50,\"~version\":2}";
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
        assertEquals("2017-11-13T21:13:59.213Z", document.getFirstUpdateAt());
        assertEquals("2017-11-13T21:13:59.213Z", document.getLastUpdateAt());
        assertEquals(2, document.getVersion());
        assertEquals("{\"author\":{\"firstName\":\"Bob\",\"lastName\":\"Johnson\"},\"title\":\"Best Ever!\",\"likes\":50}", mapper.writeValueAsString(document.getData()));
    }

    @Test
    public void testUpdateConstructor() {
        UUID timestamp = UUID.fromString("1c8f95b0-0263-11e8-8f1a-0800200c9a66");
        Map<String, Object> data = new HashMap<>();
        data.put("likes", 25);
        data.put("author", "Bob");

        Update update = new Update("posts", "abc123", timestamp, data, true);
        Document doc = new Document(update);

        assertEquals("posts", doc.getTable());
        assertEquals("abc123", doc.getKey());
        assertEquals("2017-11-13T21:13:59.213Z", document.getFirstUpdateAt());
        assertEquals("2017-11-13T21:13:59.213Z", document.getLastUpdateAt());
        assertEquals(1, doc.getVersion());
        assertEquals(data, doc.getData());
    }

    @Test
    public void testRedundantUpdate() {
        UUID timestamp = UUID.fromString("1c8f95b0-0263-11e8-8f1a-0800200c9a66");
        Map<String, Object> data = new HashMap<>();
        Map<String, Object> nested = new HashMap<>();
        nested.put("firstName", "Bob");
        nested.put("lastName", "Johnson");
        data.put("author", nested);
        data.put("title", "Best Ever!");
        data.put("likes", 50);

        Update update = new Update("comment", "dsfaf3", timestamp, data, true);

        document.addUpdate(update);

        assertEquals("comment", document.getTable());
        assertEquals("dsfaf3", document.getKey());
        assertEquals("2017-11-13T21:13:59.213Z", document.getFirstUpdateAt());
        assertEquals("2017-11-13T21:13:59.213Z", document.getLastUpdateAt());
        assertEquals(2, document.getVersion());
        assertEquals(data, document.getData());
    }

    @Test
    public void testAddUpdateWithSmashUpdate() {
        UUID timestamp = UUID.fromString("1c8f95b0-0263-11e8-8f1a-0800200c9a66");
        Map<String, Object> data = new HashMap<>();
        data.put("likes", 25);
        data.put("author", "Bob");

        Update update = new Update("comment", "dsfaf3", timestamp, data, false);
        document.addUpdate(update);

        assertEquals("comment", document.getTable());
        assertEquals("dsfaf3", document.getKey());
        assertEquals("2017-11-13T21:13:59.213Z", document.getFirstUpdateAt());
        assertEquals("2018-01-26T06:35:33.899Z", document.getLastUpdateAt());
        assertEquals(3, document.getVersion());
        assertEquals(data, document.getData());
    }

    @Test
    public void testAddUpdateWithMerge() {
        UUID timestamp = UUID.fromString("1c8f95b0-0263-11e8-8f1a-0800200c9a66");;
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
        assertEquals("2017-11-13T21:13:59.213Z", document.getFirstUpdateAt());
        assertEquals("2018-01-26T06:35:33.899Z", document.getLastUpdateAt());
        assertEquals(3, document.getVersion());
        assertEquals(expectedDataAfterMerge, document.getData());
    }

    @Test
    public void testCloneDocument() {

        UUID timestamp = UUID.fromString("1c8f95b0-0263-11e8-8f1a-0800200c9a66");;
        Map<String, Object> data = new HashMap<>();
        Map<String, Object> author = new HashMap<>();
        data.put("likes", 25);
        data.put("location", "New York");
        author.put("firstName", "Robert");
        author.put("email", "bob@yahoo.com");
        author.put("age", 22);
        data.put("author", author);

        Document docCopy = document.clone();

        Update update = new Update("comment", "dsfaf3", timestamp, data, true);
        document.addUpdate(update);

        Map<String, Object> oldData = new HashMap<>();
        Map<String, Object> nested = new HashMap<>();
        nested.put("firstName", "Bob");
        nested.put("lastName", "Johnson");
        oldData.put("author", nested);
        oldData.put("title", "Best Ever!");
        oldData.put("likes", 50);

        assertEquals("comment", docCopy.getTable());
        assertEquals("dsfaf3", docCopy.getKey());
        assertEquals("2017-11-13T21:13:59.213Z", docCopy.getFirstUpdateAt());
        assertEquals("2017-11-13T21:13:59.213Z", docCopy.getLastUpdateAt());
        assertEquals(2, docCopy.getVersion());
        assertEquals(docCopy.getData(), oldData);

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
        assertEquals("2017-11-13T21:13:59.213Z", document.getFirstUpdateAt());
        assertEquals("2018-01-26T06:35:33.899Z", document.getLastUpdateAt());
        assertEquals(3, document.getVersion());
        assertEquals(expectedDataAfterMerge, document.getData());

    }

}
