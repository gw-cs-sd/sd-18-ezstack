package org.ezstack.denormalizer.core;

import com.google.common.collect.*;
import org.apache.commons.collections4.KeyValue;
import org.apache.commons.collections4.keyvalue.DefaultKeyValue;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.ezstack.denormalizer.model.*;
import org.ezstack.ezapp.datastore.api.Document;
import org.ezstack.ezapp.datastore.api.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class DocumentMessageMapper implements FlatMapFunction<DocumentChangePair, DocumentMessage> {

    private static final Logger log = LoggerFactory.getLogger(DocumentMessageMapper.class);

    private HashMultimap<String, KeyValue<Query, QueryLevel>> _queryIndex;

    public DocumentMessageMapper(Collection<Query> queries) {
        _queryIndex = getQueryIndex(queries);
    }

    private HashMultimap<String, KeyValue<Query, QueryLevel>> getQueryIndex(Collection<Query> queries) {
        HashMultimap<String, KeyValue<Query, QueryLevel>> index = HashMultimap.create();

        for (Query query : queries) {
            index.put(query.getTable(), new DefaultKeyValue<>(query, QueryLevel.OUTER));

            if (query.getJoin() != null) {
                index.put(query.getJoin().getTable(), new DefaultKeyValue<>(query, QueryLevel.INNER));
            }
        }

        return index;
    }

    private Set<KeyValue<Query, QueryLevel>> getApplicableQueries(Document document) {

        if (document == null) {
            return ImmutableSet.of();
        }

        Set<KeyValue<Query, QueryLevel>> queryPairs = _queryIndex.get(document.getTable());

        if (queryPairs == null) {
            return ImmutableSet.of();
        }

        return Sets.filter(queryPairs, queryPair -> {
            // TODO ensure document has the requisite join attributes, else return false

            // TODO apply filter here, approve all for now
            return true;
        });
    }

    @Override
    public Collection<DocumentMessage> apply(DocumentChangePair changePair) {
        Set<KeyValue<Query, QueryLevel>> oldApplicableQueries = getApplicableQueries(changePair.getOldDocument());
        Set<KeyValue<Query, QueryLevel>> newApplicableQueries = getApplicableQueries(changePair.getNewDocument());


        Set<KeyValue<String, QueryLevel>> newPartitionLocations = newApplicableQueries
                .parallelStream()
                .map(pair -> new DefaultKeyValue<>(FanoutHashingUtils.getPartitionKey(changePair.getNewDocument(),
                        pair.getValue(), pair.getKey()), pair.getValue()))
                .collect(Collectors.toSet());



        Set<KeyValue<Query, QueryLevel>> queriesForDeletion = oldApplicableQueries
                .parallelStream()
                .filter(pair -> !newPartitionLocations.contains(new DefaultKeyValue<>(
                        FanoutHashingUtils.getPartitionKey(changePair.getOldDocument(),
                                pair.getValue(), pair.getKey()), pair.getValue())))
                .collect(Collectors.toSet());

        Collection<DocumentMessage> messages = new LinkedList<>();

        // add all the update messages
        for (KeyValue<Query, QueryLevel> queryPair : newApplicableQueries) {
            messages.add(new DocumentMessage(changePair.getNewDocument(),
                    FanoutHashingUtils.getPartitionKey(changePair.getNewDocument(),
                            queryPair.getValue(), queryPair.getKey()),
                    queryPair.getValue(), OpCode.UPDATE, queryPair.getKey()));
        }

        // add all the delete messages
        for (KeyValue<Query, QueryLevel> queryPair : queriesForDeletion) {
            messages.add(new DocumentMessage(changePair.getOldDocument(),
                    FanoutHashingUtils.getPartitionKey(changePair.getOldDocument(),
                            queryPair.getValue(), queryPair.getKey()),
                    queryPair.getValue(), OpCode.DELETE, queryPair.getKey()));
        }

        return messages;
    }
}
