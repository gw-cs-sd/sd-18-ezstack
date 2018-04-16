package org.ezstack.ezapp.datastore.core;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.ezstack.ezapp.datastore.api.*;
import org.ezstack.ezapp.datastore.db.elasticsearch.ElasticSearchDataReaderDAO;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class DefaultDataReader implements DataReader {

    private static final int DEFAULT_RETENTION_TIME_IN_MILLIS = 120000;
    private static final int DEFAULT_BATCH_SIZE = 100;

    private final ElasticSearchDataReaderDAO _dataReaderDAO;
    private final RulesManager _rulesManager;
    private final MetricRegistry _metricRegistry;
    private final Meter _ruleMeter;


    @Inject
    public DefaultDataReader(ElasticSearchDataReaderDAO dataReader, RulesManager rulesManager,
                             MetricRegistry metricRegistry) {
        Preconditions.checkNotNull(dataReader);
        Preconditions.checkNotNull(rulesManager);
        _metricRegistry = checkNotNull(metricRegistry, "metricRegistry");
        _ruleMeter = metricRegistry.meter("org.ezstack.ezapp.DefaultDataReader.ruleUsed");


        _dataReaderDAO = dataReader;
        _rulesManager = rulesManager;
    }

    @Override
    public Map<String, Object> getDocument(String table, String id) {
        return _dataReaderDAO.getDocument(table, id);
    }

    @Override
    public QueryResult getDocuments(long retentionTimeInMillis, Query query) {
        return getDocuments(retentionTimeInMillis, DEFAULT_BATCH_SIZE, query);
    }

    @Override
    public QueryResult getDocuments(int batchSize, Query query) {
        return getDocuments(DEFAULT_RETENTION_TIME_IN_MILLIS, batchSize, query);
    }

    @Override
    public QueryResult getDocuments(Query query) {
        return getDocuments(DEFAULT_RETENTION_TIME_IN_MILLIS, DEFAULT_BATCH_SIZE, query);
    }

    @Override
    public QueryResult getDocuments(long retentionTimeInMillis, int batchSize, Query query) {
        RuleExecutor ruleExecutor = new RuleExecutor(query, _rulesManager, _ruleMeter);
        return _dataReaderDAO.getDocuments(retentionTimeInMillis, batchSize, ruleExecutor);
    }
}
