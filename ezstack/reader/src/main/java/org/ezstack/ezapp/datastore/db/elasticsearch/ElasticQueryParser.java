package org.ezstack.ezapp.datastore.db.elasticsearch;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.ezstack.ezapp.datastore.api.*;

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

    QueryResult getDocuments() {
        return exec(_query);
    }

    private QueryResult exec(Query q) {
        if (q == null) {
            return new QueryResult();
        }

        Set<Document> results = new HashSet<>();
        BoolQueryBuilder boolQuery = getFilterBoolQueryBuilder(QueryHelper.safeSet(q.getFilters()));
        SearchResponse response;

        try {
            response = _client.prepareSearch(q.getTable())
                    .setScroll(new TimeValue(_scrollInMillis))
                    .setTypes(q.getTable())
                    .setSize(_batchSize)
                    .setQuery(boolQuery)
                    .get();
        } catch (IndexNotFoundException e) {
            return new QueryResult();
        }

        SearchHitIterator iter = new SearchHitIterator(_client, response);
        Set<SearchTypeAggregationHelper> helpers = QueryHelper.createAggHelpers(q.getSearchTypes());
        // if search types is empty then defaults to getting documents
        boolean userWantsDocuments = q.getSearchTypes().isEmpty() || QueryHelper.hasSearchRequest(q.getSearchTypes());

        while (iter.hasNext()) {
            SearchHit searchHit = iter.next();
            Document doc = new Document(searchHit.getSourceAsMap());
            if (q.getJoin() != null) {
                Query innerJoin = q.getJoin();
                Set<Filter> innerJoinFilters = innerJoin.getFilters().isEmpty() ? new HashSet<>() : innerJoin.getFilters();
                innerJoinFilters.addAll(convertJoinAttributesToFilters(doc, q.getJoinAttributes()));

                QueryHelper.updateAggHelpers(helpers, doc);
                doc = QueryHelper.filterAttributes(q.getExcludeAttributes(), q.getIncludeAttributes(), doc);

                doc.setDataField(q.getJoinAttributeName(),
                        exec(new Query(innerJoin.getSearchTypes(),
                                innerJoin.getTable(),
                                innerJoinFilters,
                                innerJoin.getJoin(),
                                innerJoin.getJoinAttributeName(),
                                innerJoin.getJoinAttributes(),
                                innerJoin.getExcludeAttributes(),
                                innerJoin.getIncludeAttributes())));
            } else {
                QueryHelper.updateAggHelpers(helpers, doc);
                doc = QueryHelper.filterAttributes(q.getExcludeAttributes(), q.getIncludeAttributes(), doc);
            }

            if (userWantsDocuments) {
                results.add(doc);
            }
        }

        QueryResult queryResult = new QueryResult();
        queryResult.addAggregations(helpers);

        if (userWantsDocuments) {
            queryResult.addDocuments(results);
        }

        return queryResult;
    }

    private BoolQueryBuilder getFilterBoolQueryBuilder(Set<Filter> filters) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        for (Filter f: filters) {
            switch (f.getOp()) {
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

    private List<Filter> convertJoinAttributesToFilters(Document doc, Set<JoinAttribute> attributes) {
        List<Filter> filters = new LinkedList<>();
        for (JoinAttribute ma: attributes) {
            if (doc.containsKey(ma.getOuterAttribute())) {
                String fa = ma.getInnerAttribute();
                Object val = doc.getValue(ma.getOuterAttribute());
                filters.add(new Filter(fa, Filter.Operation.EQ, val));
            }
        }
        return filters;
    }
}
