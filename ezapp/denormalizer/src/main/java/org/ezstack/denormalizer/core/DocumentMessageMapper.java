package org.ezstack.denormalizer.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.*;
import org.apache.commons.collections4.KeyValue;
import org.apache.commons.collections4.keyvalue.DefaultKeyValue;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.ezstack.denormalizer.model.*;
import org.ezstack.ezapp.datastore.api.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.ezstack.denormalizer.model.DocumentMessage.OpCode;

public class DocumentMessageMapper implements FlatMapFunction<DocumentChangePair, DocumentMessage> {

    private static final HashSet<KeyValue<Query, QueryLevel>> EMPTY_HASH_SET = new HashSet<>();

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
            return EMPTY_HASH_SET;
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

        Set<KeyValue<Query, QueryLevel>> queriesForDeletion = Sets.filter(oldApplicableQueries,
                queryPair -> !newApplicableQueries.contains(queryPair));

        Collection<DocumentMessage> messages = new LinkedList<>();

        // add all the update messages
        for (KeyValue<Query, QueryLevel> queryPair : newApplicableQueries) {
            messages.add(new DocumentMessage(changePair.getNewDocument(),
                    FanoutHashingUtils.getPartitionKey(changePair.getNewDocument(),
                            queryPair.getValue(), queryPair.getKey().getJoinAttributes()),
                    queryPair.getValue(), OpCode.UPDATE));
        }

        // Instead of sending the the full document in a DELETE message, instead send a stripped
        // down one which is smaller in size, as the data isn't necessary for deletion
        Document deletedDocument = changePair.getOldDocument() != null ?
                new Document(changePair.getOldDocument().getTable(),
                        changePair.getOldDocument().getKey())
                : null;

        // add all the delete messages
        for (KeyValue<Query, QueryLevel> queryPair : queriesForDeletion) {
            messages.add(new DocumentMessage(deletedDocument,
                    FanoutHashingUtils.getPartitionKey(changePair.getOldDocument(),
                            queryPair.getValue(), queryPair.getKey().getJoinAttributes()),
                    queryPair.getValue(), OpCode.DELETE));
        }

        return messages;
    }
}
