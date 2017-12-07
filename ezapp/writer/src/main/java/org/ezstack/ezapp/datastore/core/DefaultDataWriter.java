package org.ezstack.ezapp.datastore.core;

import com.google.inject.Inject;
import org.ezstack.ezapp.datastore.api.DataWriter;
import org.ezstack.ezapp.datastore.api.Update;
import org.ezstack.ezapp.datastore.db.kafka.KafkaDataWriterDAO;

import java.util.Map;

public class DefaultDataWriter implements DataWriter {

    private final KafkaDataWriterDAO _dataWriterDAO;

    @Inject
    public DefaultDataWriter(KafkaDataWriterDAO dataWriterDAO) {
        _dataWriterDAO = dataWriterDAO;
    }

    @Override
    public void create(String database, String table, String key, Map<String, Object> document) {
        _dataWriterDAO.update(new Update(database, table, key, null, document, false));
    }

    @Override
    public void update(String database, String table, String key, Map<String, Object> update) {
        _dataWriterDAO.update(new Update(database, table, key, null, update, true));
    }
}
