package org.ezstack.ezapp.datastore.core;

import com.google.inject.Inject;
import org.ezstack.ezapp.datastore.api.DataReader;
import org.ezstack.ezapp.datastore.api.Query;
import org.ezstack.ezapp.datastore.db.elasticsearch.ElasticSearchDataReader;

import java.util.List;
import java.util.Map;

public class DefaultDataReader implements DataReader {
    private final ElasticSearchDataReader _elasticSearchDataReader;

    @Inject
    public DefaultDataReader(ElasticSearchDataReader elasticSearchDataReader) {
        _elasticSearchDataReader = elasticSearchDataReader;
    }

    @Override
    public Map<String, Object> getDocument(String database, String table, String id) {
        return _elasticSearchDataReader.getDocument(database, table, id);
    }

    @Override
    public List<Map<String, Object>> getDocuments(Query query) {
        return _elasticSearchDataReader.getDocuments(query);
    }
}
