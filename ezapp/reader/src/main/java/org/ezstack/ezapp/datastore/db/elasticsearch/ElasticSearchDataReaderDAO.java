package org.ezstack.ezapp.datastore.db.elasticsearch;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.ezstack.ezapp.datastore.api.Query;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ElasticSearchDataReaderDAO {
    private final Client _client;

    @Inject
    public ElasticSearchDataReaderDAO(Client client) {
        Preconditions.checkNotNull(client);

        _client = client;
    }

    public Map<String, Object> getDocument(String index, String id) {
        boolean hasIndex = _client.admin().indices().prepareExists(index).get().isExists();
        if (hasIndex == false) {
            return Collections.emptyMap();
        }
        GetResponse response = _client.prepareGet(index, index, id).get();
        return response.getSourceAsMap();
    }

    public List<Map<String, Object>> getDocuments(Query query) {
        // TODO
        return null;
    }
}
