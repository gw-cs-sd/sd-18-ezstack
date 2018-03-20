package org.ezstack.ezapp.datastore.core;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.ezstack.ezapp.datastore.api.*;
import org.ezstack.ezapp.datastore.db.elasticsearch.ElasticSearchDataReaderDAO;

import java.util.Map;

public class DefaultDataReader implements DataReader {
    private final ElasticSearchDataReaderDAO _dataReaderDAO;
    private final RulesManager _rulesManager;

    @Inject
    public DefaultDataReader(ElasticSearchDataReaderDAO dataReader, RulesManager rulesManager) {
        Preconditions.checkNotNull(dataReader);
        Preconditions.checkNotNull(rulesManager);

        _dataReaderDAO = dataReader;
        _rulesManager = rulesManager;
    }

    @Override
    public Map<String, Object> getDocument(String table, String id) {
        return _dataReaderDAO.getDocument(table, id);
    }

    @Override
    public QueryResult getDocuments(long scrollInMillis, int batchSize, Query query) {
        RuleExecutor ruleExecutor = new RuleExecutor(query, _rulesManager);
        return _dataReaderDAO.getDocuments(scrollInMillis, batchSize, ruleExecutor);
    }
}
