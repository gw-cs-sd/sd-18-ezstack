package org.ezstack.ezapp.datastore.db.elasticsearch;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.ezstack.ezapp.datastore.api.DataReader;
import org.ezstack.ezapp.datastore.api.Query;

import java.util.List;
import java.util.Map;

public class ElasticSearchDataReader implements DataReader {
    private final Client _client;

    @Inject
    public ElasticSearchDataReader (Client client) {
        Preconditions.checkNotNull(client);

        _client = client;
    }

    @Override
    public Map<String, Object> getDocument(String index, String type, String id) {
        GetResponse response = _client.prepareGet(index, type, id).get();
        return response.getSourceAsMap();
    }

    @Override
    public List<Map<String, Object>> getDocuments(Query query) {
        // TODO
        return null;
    }
}
