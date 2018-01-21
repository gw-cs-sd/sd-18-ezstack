package org.ezstack.ezapp.datastore.db.elasticsearch;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.search.SearchHit;
import org.ezstack.ezapp.datastore.api.Query;

import java.util.*;

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

    public List<Map<String, Object>> getDocuments(Query query) {
        try {
            List<Map<String, Object>> results = new ArrayList<>();
            SearchResponse response = _client.prepareSearch(query.getTable())
                    .setScroll(new TimeValue(60000*2))
                    .setTypes(query.getTable())
                    .setSize(100)
                    .get();

            SearchHitIterator iter = new SearchHitIterator(_client, response);

            while (iter.hasNext()) {
                SearchHit searchHit = iter.next();
                // TODO: do a secondary search to get the inner attribute and filter based on contains and equality
                results.add(searchHit.getSourceAsMap());
            }

            return results;
        } catch (IndexNotFoundException e) {
            return Collections.emptyList();
        }
    }
}
