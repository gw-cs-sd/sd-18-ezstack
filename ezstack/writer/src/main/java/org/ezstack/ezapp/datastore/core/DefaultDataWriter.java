package org.ezstack.ezapp.datastore.core;

import com.google.inject.Inject;
import org.ezstack.ezapp.datastore.api.DataWriter;
import org.ezstack.ezapp.datastore.api.Update;
import org.ezstack.ezapp.datastore.db.kafka.KafkaDataWriterDAO;

import java.util.Map;
import java.util.UUID;

public class DefaultDataWriter implements DataWriter {

    private final KafkaDataWriterDAO _dataWriterDAO;

    @Inject
    public DefaultDataWriter(KafkaDataWriterDAO dataWriterDAO) {
        _dataWriterDAO = dataWriterDAO;
    }

    @Override
    public String create(String table, String key, Map<String, Object> document) {
        _dataWriterDAO.update(new Update(table, key, null, document, false));
        return key;
    }

    @Override
    public String create(String table, Map<String, Object> document) {
        String key = UUID.randomUUID().toString();
        create(table, key, document);
        return key;
    }

    @Override
    public String update(String table, String key, Map<String, Object> update) {
        _dataWriterDAO.update(new Update(table, key, null, update, true));
        return key;
    }
}
