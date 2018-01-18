package org.ezstack.ezapp.datastore.db.elasticsearch;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
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
            SearchResponse response = _client.prepareSearch(query.getOuterTable())
                    .setTypes(query.getOuterTable())
                    .get();
            SearchHits hits = response.getHits();
            Iterator<SearchHit> iter = hits.iterator();

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
