package org.ezstack.ezapp.datastore.core;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.ezstack.ezapp.datastore.api.DataReader;
import org.ezstack.ezapp.datastore.api.Query;

import java.util.List;
import java.util.Map;

public class DefaultDataReader implements DataReader {
    private final DataReader _dataReader;

    @Inject
    public DefaultDataReader(DataReader dataReader) {
        Preconditions.checkNotNull(dataReader);

        _dataReader = dataReader;
    }

    @Override
    public Map<String, Object> getDocument(String database, String table, String id) {
        return _dataReader.getDocument(database, table, id);
    }

    @Override
    public List<Map<String, Object>> getDocuments(Query query) {
        return _dataReader.getDocuments(query);
    }
}
