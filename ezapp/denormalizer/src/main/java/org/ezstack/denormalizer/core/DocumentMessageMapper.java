package org.ezstack.denormalizer.core;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.ezstack.denormalizer.model.Document;
import org.ezstack.denormalizer.model.DocumentMessage;
import org.ezstack.ezapp.datastore.api.JoinAttribute;
import org.ezstack.ezapp.datastore.api.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.ezstack.ezapp.datastore.api.KeyBuilder.hash;
import static org.ezstack.ezapp.datastore.api.KeyBuilder.hashKey;

public class DocumentMessageMapper implements FlatMapFunction<Document, DocumentMessage> {

    private static final Logger log = LoggerFactory.getLogger(DocumentMessageMapper.class);

    private Map<String, Collection<Query>> _queryIndex;

    public DocumentMessageMapper(Collection<Query> queries) {
        _queryIndex = getQueryIndex(queries);
    }

    private Map<String, Collection<Query>> getQueryIndex(Collection<Query> queries) {
        Map<String, Collection<Query>> index = new HashMap<>();
        for (Query query : queries) {
            Query outerQuery = query;
            while (query != null) {
                String tableKey = query.getTable();
                Collection<Query> queriesForTable = MoreObjects.firstNonNull(index.get(tableKey), new LinkedList<>());
                queriesForTable.add(outerQuery);
                index.put(tableKey, queriesForTable);
                query = query.getJoin();
            }
        }
        return index;
    }

    @Override
    public Collection<DocumentMessage> apply(Document document) {
        String table = document.getTable();
        Collection<Query> applicableQueries = MoreObjects.firstNonNull(_queryIndex.get(table), ImmutableSet.of());
        Collection<DocumentMessage> messages = new LinkedList<>();

        queryLoop:
        for (Query query : applicableQueries) {

            // apply filters here, and if it is not applicable, then continue

            if (query.getJoin() == null) {
                messages.add(new DocumentMessage(document, hashKey(query.getTable(), document.getKey())));
                continue;
            }


            if (query.getTable().equals(document.getTable())) {
                // TODO: make this hash function avoid collisions
                String[] valuesForKey = new String[query.getJoinAttributes().size()];
                Iterator<JoinAttribute> atts = query.getJoinAttributes().iterator();
                for (int i = 0; i < valuesForKey.length; i++) {
                    valuesForKey[i] = document.getValue(atts.next().getOuterAttribute()).toString();
                    if (valuesForKey[i] == null) continue queryLoop;
                }
                messages.add(new DocumentMessage(document, hash(valuesForKey)));
            }

            if (query.getJoin().getTable().equals(document.getTable())) {
                // TODO: make this hash function avoid collisions
                String[] valuesForKey = new String[query.getJoinAttributes().size()];
                Iterator<JoinAttribute> atts = query.getJoinAttributes().iterator();
                for (int i = 0; i < valuesForKey.length; i++) {
                    valuesForKey[i] = document.getValue(atts.next().getInnerAttribute()).toString();
                    if (valuesForKey[i] == null) continue queryLoop;
                }
                messages.add(new DocumentMessage(document, hash(valuesForKey)));
            }
        }

        return messages;

    }
}
