package org.ezstack.ezapp.datastore.db.elasticsearch;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.ezstack.ezapp.datastore.api.MatchAttribute;
import org.ezstack.ezapp.datastore.api.Query;

import java.util.LinkedList;
import java.util.Collections;
import java.util.Map;
import java.util.List;

public class ElasticQueryParser {
    private long _scrollInMillis;
    private int _batchSize;
    private Query _query;
    private Client _client;

    public ElasticQueryParser(long scrollInMillis, int batchSize, Query query, Client client) {
        _scrollInMillis = scrollInMillis;
        _batchSize = batchSize;
        _query = query;
        _client = client;
    }

    List<Map<String, Object>> getDocuments() {
        try {
            List<MatchAttribute> matchAttributes = _query.getMatchAttribute();
            List<Map<String, Object>> results = new LinkedList<>();
            Query join = _query.getJoin();

            SearchResponse response = _client.prepareSearch(_query.getTable())
                    .setScroll(new TimeValue(_scrollInMillis))
                    .setTypes(_query.getTable())
                    .setSize(_batchSize)
                    .get();
            SearchHitIterator iter = new SearchHitIterator(_client, response);

            while (iter.hasNext()) {
                SearchHit searchHit = iter.next();
                Map<String, Object> doc = searchHit.getSourceAsMap();
                List<Map<String, Object>> nestedDocs = new LinkedList<>();

                SearchRequestBuilder builder = _client.prepareSearch(join.getTable())
                        .setSize(_batchSize)
                        .setScroll(new TimeValue(_scrollInMillis))
                        .setTypes(join.getTable());
                BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
                for (MatchAttribute attribute: matchAttributes) {
                    if (doc.containsKey(attribute.getOuterAttribute())) {
                        boolQuery.must(QueryBuilders.termQuery(
                                attribute.getInnerAttribute(), doc.get(attribute.getOuterAttribute())));
                    }
                }
                SearchHitIterator joinIter = new SearchHitIterator(_client, builder.setQuery(boolQuery).get());

                while (joinIter.hasNext()) {
                    SearchHit nestedHit = joinIter.next();
                    nestedDocs.add(nestedHit.getSourceAsMap());
                }

                doc.put(_query.getJoinAttribute(),nestedDocs);
                results.add(doc);
            }

            return results;
        } catch (IndexNotFoundException e) {
            return Collections.emptyList();
        }
    }
}
