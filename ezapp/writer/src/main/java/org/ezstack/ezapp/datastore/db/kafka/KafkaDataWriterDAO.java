package org.ezstack.ezapp.datastore.db.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.ezstack.ezapp.datastore.api.Update;

import static com.google.common.base.Preconditions.checkNotNull;

public class KafkaDataWriterDAO {

    private final Producer<String, JsonNode> _producer;
    private final String _documentTopic;
    private final ObjectMapper _objectMapper;

    @Inject
    public KafkaDataWriterDAO(Producer<String, JsonNode> producer, @Named("documentTopic") String documentTopic) {
        checkNotNull(producer, "producer");
        checkNotNull(documentTopic, "documentTopic");

        _producer = producer;
        _documentTopic = documentTopic;
        _objectMapper = new ObjectMapper();
    }

    public void update(Update update) {
        // TODO: need hash function for ids that considers database and table
        try {
            RecordMetadata metadata = _producer.send(new ProducerRecord<String, JsonNode>(_documentTopic, update.getKey(),
                    _objectMapper.valueToTree(update))).get();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }
}
