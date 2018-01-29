package org.ezstack.ezapp.datastore.db.elasticsearch;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.ezstack.ezapp.datastore.api.DataType;
import org.ezstack.ezapp.datastore.api.Filter;
import org.ezstack.ezapp.datastore.api.JoinAttribute;
import org.ezstack.ezapp.datastore.api.Query;

import java.util.*;

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
            return join(_query);
        } catch (IndexNotFoundException e) {
            HashMap<String, Object> err = new HashMap<>();
            err.put("error", "IndexNotFoundException");
            List<Map<String, Object>> errList = new LinkedList<>();
            errList.add(err);
            return errList;
        }
    }

    private List<Map<String, Object>> join(Query outerQuery) {
        // ensure their is a query
        if (outerQuery == null) {
            return Collections.emptyList();
        }

        List<Map<String, Object>> results = new LinkedList<>();
        BoolQueryBuilder boolQuery = getFilterBoolQueryBuilder(safe(outerQuery.getFilters()));

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
            if (outerQuery.getJoin() != null) {
                Query innerJoin = outerQuery.getJoin();
                List<Filter> innerJoinFilters = innerJoin.getFilters();
                innerJoinFilters.addAll(convertMatchAttributesToFilters(doc, outerQuery.getJoinAttributes()));
                doc.put(outerQuery.getJoinAttributeName(),
                        join(new Query(innerJoin.getAggregationType(),
                                innerJoin.getAggregationAttributeName(),
                                innerJoin.getTable(),
                                innerJoinFilters,
                                innerJoin.getJoin(),
                                innerJoin.getJoinAttributeName(),
                                innerJoin.getJoinAttributes())));
            }
            results.add(doc);
        }

        return results;
    }

    private BoolQueryBuilder getFilterBoolQueryBuilder(List<Filter> filters) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        for (Filter f: filters) {
            switch (f.getOpt()) {
                case EQ:
                    boolQuery.must(QueryBuilders.termsQuery(f.getAttribute(), f.getValue()));
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

    private List<Filter> convertMatchAttributesToFilters(Map<String, Object> doc, List<JoinAttribute> attributes) {
        List<Filter> filters = new LinkedList<>();
        for (JoinAttribute ma: attributes) {
            if (doc.containsKey(ma.getOuterAttribute())) {
                String fa = ma.getInnerAttribute();
                Object val = doc.get(ma.getOuterAttribute());
                filters.add(new Filter(fa, Filter.Operations.EQ, val));
            }
        }
        return filters;
    }

    private static List safe(List l) {
        return l == null ? Collections.emptyList() : l;
    }
}
