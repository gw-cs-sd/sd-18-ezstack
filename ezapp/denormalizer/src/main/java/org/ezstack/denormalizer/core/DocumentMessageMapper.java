package org.ezstack.denormalizer.core;

import com.google.common.collect.*;
import org.apache.commons.collections4.KeyValue;
import org.apache.commons.collections4.keyvalue.DefaultKeyValue;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.ezstack.denormalizer.model.*;
import org.ezstack.ezapp.datastore.api.Document;
import org.ezstack.ezapp.datastore.api.Query;
import org.ezstack.ezapp.datastore.api.QueryHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class DocumentMessageMapper implements FlatMapFunction<DocumentChangePair, DocumentMessage> {

    private static final Logger log = LoggerFactory.getLogger(DocumentMessageMapper.class);

    private HashMultimap<String, QueryPair> _queryIndex;

    public DocumentMessageMapper(Collection<Query> queries) {
        _queryIndex = getQueryIndex(queries);
    }

    private HashMultimap<String, QueryPair> getQueryIndex(Collection<Query> queries) {
        HashMultimap<String, QueryPair> index = HashMultimap.create();

        for (Query query : queries) {
            index.put(query.getTable(), new QueryPair(query, QueryLevel.OUTER));

            if (query.getJoin() != null) {
                index.put(query.getJoin().getTable(), new QueryPair(query, QueryLevel.INNER));
            }
        }

        return index;
    }

    private boolean doesMatchQuery(QueryPair queryPair, Document doc) {
        Query q = queryPair.getLevel() == QueryLevel.OUTER ? queryPair.getQuery() : queryPair.getQuery().getJoin();

        return QueryHelper.meetsFilters(q.getFilters(), doc);
    }

    private boolean documentHasJoinAttributeValues(QueryPair queryPair, Document doc) {
        if (queryPair.getQuery().getJoinAttributes() == null) {
            return true;
        }
        return queryPair.getQuery().getJoinAttributes()
                .stream()
                .map(att -> queryPair.getLevel() == QueryLevel.OUTER ? att.getOuterAttribute() : att.getInnerAttribute())
                .allMatch(att ->  doc.getValue(att) != null);
    }

    private Set<QueryPair> getApplicableQueries(Document document) {

        if (document == null) {
            return ImmutableSet.of();
        }

        Set<QueryPair> queryPairs = _queryIndex.get(document.getTable());

        if (queryPairs == null) {
            return ImmutableSet.of();
        }

        return queryPairs
                .stream()
                .filter(pair -> documentHasJoinAttributeValues(pair, document))
                .filter(pair -> doesMatchQuery(pair, document))
                .collect(Collectors.toSet());
    }

    @Override
    public Collection<DocumentMessage> apply(DocumentChangePair changePair) {
        Set<QueryPair> oldApplicableQueries = getApplicableQueries(changePair.getOldDocument());
        Set<QueryPair> newApplicableQueries = getApplicableQueries(changePair.getNewDocument());

        Set<KeyValue<String, QueryLevel>> newPartitionLocations = newApplicableQueries
                .stream()
                .map(pair -> new DefaultKeyValue<>(FanoutHashingUtils.getPartitionKey(changePair.getNewDocument(),
                        pair.getLevel(), pair.getQuery()), pair.getLevel()))
                .collect(Collectors.toSet());

        Set<QueryPair> queriesForDeletion = oldApplicableQueries
                .stream()
                .filter(pair -> !newPartitionLocations.contains(new DefaultKeyValue<>(
                        FanoutHashingUtils.getPartitionKey(changePair.getOldDocument(),
                                pair.getLevel(), pair.getQuery()), pair.getLevel())))
                .collect(Collectors.toSet());

        Collection<DocumentMessage> messages = new LinkedList<>();

        // add all the update messages
        for (QueryPair queryPair : newApplicableQueries) {
            messages.add(new DocumentMessage(changePair.getNewDocument(),
                    FanoutHashingUtils.getPartitionKey(changePair.getNewDocument(),
                            queryPair.getLevel(), queryPair.getQuery()),
                    queryPair.getLevel(), OpCode.UPDATE, queryPair.getQuery()));
        }

        // add all the delete messages
        for (QueryPair queryPair : queriesForDeletion) {
            if (!newApplicableQueries.contains(queryPair) && queryPair.getLevel() == QueryLevel.OUTER) {
                messages.add(new DocumentMessage(changePair.getOldDocument(),
                        FanoutHashingUtils.getPartitionKey(changePair.getOldDocument(),
                                queryPair.getLevel(), queryPair.getQuery()),
                        queryPair.getLevel(), OpCode.REMOVE_AND_DELETE, queryPair.getQuery()));
            } else {
                messages.add(new DocumentMessage(changePair.getOldDocument(),
                        FanoutHashingUtils.getPartitionKey(changePair.getOldDocument(),
                                queryPair.getLevel(), queryPair.getQuery()),
                        queryPair.getLevel(), OpCode.REMOVE, queryPair.getQuery()));
            }
        }

        return messages;
    }

    private class QueryPair {
        private final Query _query;
        private final QueryLevel _queryLevel;

        public QueryPair(Query query, QueryLevel queryLevel) {
            this._query = query;
            this._queryLevel = queryLevel;
        }

        public Query getQuery() {
            return _query;
        }

        public QueryLevel getLevel() {
            return _queryLevel;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof QueryPair)) return false;
            QueryPair queryPair = (QueryPair) o;
            return _query.equals(queryPair.getQuery()) &&
                    _queryLevel == queryPair.getLevel();
        }
    }
}
