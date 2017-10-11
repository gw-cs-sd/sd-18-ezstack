package org.ezstack.ezapp.writer.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.ezstack.ezapp.writer.api.DataWriter;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;

public class DefaultDataWriter implements DataWriter {

    private final Producer<String, JsonNode> _producer;

    @Inject
    public DefaultDataWriter(Producer<String, JsonNode> producer) {
        _producer = producer;
        create("test", "1234", new HashMap<String, Object>());
    }

    public void create(String table, String key, Map<String, Object> document) {
        _producer.send(new ProducerRecord(table, key, new ObjectMapper().valueToTree(document)));
    }
}
