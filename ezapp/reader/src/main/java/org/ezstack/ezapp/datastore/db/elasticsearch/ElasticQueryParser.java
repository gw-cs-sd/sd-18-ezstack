package org.ezstack.ezapp.datastore.db.elasticsearch;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.ezstack.ezapp.datastore.api.Filter;
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
            return join(_query, _query.getJoin());
        } catch (IndexNotFoundException e) {
            return Collections.emptyList();
        }
    }

    private List<Map<String, Object>> join(Query outerQuery, Query innerQuery) {
        // ensure their is a query
        if (outerQuery == null) {
            return Collections.emptyList();
        }

        List<MatchAttribute> matchAttributes = outerQuery.getMatchAttributes();
        List<Filter> filters = outerQuery.getFilters();
        List<Map<String, Object>> results = new LinkedList<>();
        BoolQueryBuilder boolQuery = getFilterBoolQueryBuilder(filters);

        SearchResponse response = _client.prepareSearch(outerQuery.getTable())
                .setScroll(new TimeValue(_scrollInMillis))
                .setTypes(outerQuery.getTable())
                .setSize(_batchSize)
                .setQuery(boolQuery)
                .get();
        SearchHitIterator iter = new SearchHitIterator(_client, response);

        while (iter.hasNext()) {
            SearchHit searchHit = iter.next();
            Map<String, Object> doc = searchHit.getSourceAsMap();
            if (innerQuery != null) {
                doc.put(outerQuery.getJoinAttribute(), getNestedDocs(innerQuery, doc, matchAttributes));
            }
            results.add(doc);
        }

        return results;
    }

    private List<Map<String, Object>> getNestedDocs(Query innerQuery,
                                                    Map<String, Object> doc,
                                                    List<MatchAttribute> matchAttributes) {
        List<Map<String, Object>> nestedDocs = new LinkedList<>();

        SearchRequestBuilder builder = _client.prepareSearch(innerQuery.getTable())
                .setSize(_batchSize)
                .setScroll(new TimeValue(_scrollInMillis))
                .setTypes(innerQuery.getTable());
        BoolQueryBuilder boolQuery = getFilterBoolQueryBuilder(innerQuery.getFilters());
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

        return nestedDocs;
    }

    private BoolQueryBuilder getFilterBoolQueryBuilder(List<Filter> filters) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        if (filters == null) {
            return boolQuery;
        }

        for (Filter f: filters) {
            switch (f.getOpt()) {
                case EQ:
                    boolQuery.must(QueryBuilders.termQuery(f.getAttribute(), f.getValue()));
                    break;
                case GT:
                    boolQuery.must(QueryBuilders.rangeQuery(f.getAttribute()).gt(f.getValue()));
                    break;
                case LT:
                    boolQuery.must(QueryBuilders.rangeQuery(f.getAttribute()).lt(f.getValue()));
                    break;
                case GTE:
                    boolQuery.must(QueryBuilders.rangeQuery(f.getAttribute()).gte(f.getValue()));
                    break;
                case LTE:
                    boolQuery.must(QueryBuilders.rangeQuery(f.getAttribute()).lte(f.getValue()));
                    break;
                case NOT_EQ:
                    boolQuery.mustNot(QueryBuilders.termQuery(f.getAttribute(), f.getValue()));
                    break;
            }
        }

        return boolQuery;
    }
}
