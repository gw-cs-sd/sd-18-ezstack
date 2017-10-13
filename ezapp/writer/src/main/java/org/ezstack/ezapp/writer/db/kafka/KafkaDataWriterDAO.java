package org.ezstack.ezapp.writer.db.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.ezstack.ezapp.writer.api.Update;

public class KafkaDataWriterDAO {

    private final Producer<String, JsonNode> _producer;
    private final ObjectMapper _objectMapper;

    @Inject
    public KafkaDataWriterDAO(Producer<String, JsonNode> producer) {
        _producer = producer;
        _objectMapper = new ObjectMapper();
    }

    public void update(Update update) {
        try {
            RecordMetadata metadata = _producer.send(new ProducerRecord<String, JsonNode>(update.getTable(), update.getKey(),
                    _objectMapper.valueToTree(update))).get();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }
}
