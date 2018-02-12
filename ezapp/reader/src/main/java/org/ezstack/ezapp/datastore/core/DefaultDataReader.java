package org.ezstack.ezapp.datastore.core;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.ezstack.ezapp.datastore.api.DataReader;
import org.ezstack.ezapp.datastore.api.Query;
import org.ezstack.ezapp.datastore.db.elasticsearch.ElasticSearchDataReaderDAO;

import java.util.Map;

public class DefaultDataReader implements DataReader {
    private final ElasticSearchDataReaderDAO _dataReaderDAO;

    @Inject
    public DefaultDataReader(ElasticSearchDataReaderDAO dataReader) {
        Preconditions.checkNotNull(dataReader);

        _dataReaderDAO = dataReader;
    }

    @Override
    public Map<String, Object> getDocument(String table, String id) {
        return _dataReaderDAO.getDocument(table, id);
    }

    @Override
    public Map<String, Object> getDocuments(long scrollInMillis, int batchSize, Query query) {
        return _dataReaderDAO.getDocuments(scrollInMillis, batchSize, query);
    }
}
