package org.ezstack.ezapp.datastore.db.elasticsearch;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.IndexNotFoundException;
import org.ezstack.ezapp.datastore.api.Query;
import org.ezstack.ezapp.datastore.api.QueryResult;
import org.ezstack.ezapp.datastore.api.RuleExecutor;

import java.util.Map;
import java.util.Collections;

public class ElasticSearchDataReaderDAO {
    private final Client _client;

    @Inject
    public ElasticSearchDataReaderDAO(Client client) {
        Preconditions.checkNotNull(client);

        _client = client;
    }

    public Map<String, Object> getDocument(String index, String id) {
        try {
            GetResponse response = _client.prepareGet(index, index, id).get();
            return response.getSourceAsMap();
        } catch (IndexNotFoundException e) {
            return Collections.emptyMap();
        }
    }

    public QueryResult getDocuments(long scrollInMillis, int batchSize, RuleExecutor ruleExecutor) {
        return new ElasticQueryParser(scrollInMillis, batchSize, ruleExecutor, _client).getDocuments();
    }
}
