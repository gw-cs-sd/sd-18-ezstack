package org.ezstack.ezapp.writer.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.ezstack.ezapp.writer.api.DataWriter;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.ezstack.ezapp.writer.api.Update;
import org.ezstack.ezapp.writer.db.kafka.KafkaDataWriterDAO;

import java.util.Map;

public class DefaultDataWriter implements DataWriter {

    private final KafkaDataWriterDAO _dataWriterDAO;

    @Inject
    public DefaultDataWriter(KafkaDataWriterDAO dataWriterDAO) {
        _dataWriterDAO = dataWriterDAO;
    }

    public void create(String table, String key, Map<String, Object> document) {
        _dataWriterDAO.update(new Update(table, key, null, document));
    }
}
