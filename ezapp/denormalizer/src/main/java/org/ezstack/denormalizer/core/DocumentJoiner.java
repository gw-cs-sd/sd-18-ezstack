package org.ezstack.denormalizer.core;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;
import org.apache.samza.config.Config;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.task.TaskContext;
import org.ezstack.denormalizer.model.*;
import org.ezstack.ezapp.datastore.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

public class DocumentJoiner implements FlatMapFunction<DocumentMessage, WritableResult> {

    private static final Logger log = LoggerFactory.getLogger(DocumentJoiner.class);

    private final String _storeName;
    private KeyValueStore<String, JoinQueryIndex> _store;


    public DocumentJoiner(String storeName) {
        _storeName = storeName;
    }

    @Override
    public void init(Config config, TaskContext context) {
        _store = (KeyValueStore<String, JoinQueryIndex>) context.getStore(_storeName);
    }

    private Collection<WritableResult> getDenormalizationForDocuments(List<Document> outerDocs,
                                                                      List<Document> innerDocs, Query query) {

        checkNotNull(query, "query");
        checkNotNull(query.getJoin(), "query join");

        List<SearchType> searchTypes = query.getJoin().getSearchTypes();

        QueryResult queryResult = new QueryResult();

        boolean userWantsDocuments = searchTypes == null || searchTypes.isEmpty()
                || QueryHelper.hasSearchRequest(searchTypes);

        if (userWantsDocuments) {
            queryResult.addDocuments(innerDocs);
        }

        List<SearchTypeAggregationHelper> helpers = QueryHelper.createAggHelpers(searchTypes);
        innerDocs.forEach(doc -> QueryHelper.updateAggHelpers(helpers, doc));


        queryResult.addAggregations(helpers);

        return outerDocs
                .stream()
                .map(outerDoc -> {
                    outerDoc = outerDoc.clone();
                    outerDoc.setDataField(query.getJoinAttributeName(), queryResult);
                    return outerDoc;
                })
                .map(denormDoc -> new WritableResult(denormDoc, query.getMurmur3HashAsString(),
                        WritableResult.Action.INDEX))
                .collect(Collectors.toSet());
    }

    private Collection<WritableResult> getActionsForRemove(JoinQueryIndex joinQueryIndex, DocumentMessage message) {
        joinQueryIndex.deleteDocument(message.getDocument(), message.getDocumentLevel());

        List<Document> outerDocs = joinQueryIndex.getEffectedDocumentsOuter();
        List<Document> innerDocs = joinQueryIndex.getEffectedDocumentsInner();
        joinQueryIndex.refresh();
        
        switch (message.getDocumentLevel()) {
            case OUTER:
                if (message.getOpCode() == OpCode.REMOVE) {
                    return ImmutableSet.of();
                }
                return outerDocs
                        .stream()
                        .map(docToDelete -> new WritableResult(docToDelete, message.getQuery().getMurmur3HashAsString(),
                                WritableResult.Action.DELETE))
                        .collect(Collectors.toSet());
            case INNER:
                return getDenormalizationForDocuments(outerDocs, innerDocs, message.getQuery());
//                return outerDocs
//                    .stream()
//                    .map(outerDoc -> {
//                        if (message.getQuery().getJoin() != null) {
//                            outerDoc = outerDoc.clone();
//                            outerDoc.setDataField(message.getQuery().getJoinAttributeName(), innerDocs);
//                        }
//                        return outerDoc;
//                    })
//                    .map(denormDoc -> new WritableResult(denormDoc, message.getQuery().getMurmur3HashAsString(),
//                            WritableResult.Action.INDEX))
//                    .collect(Collectors.toSet());
        }

        return ImmutableSet.of();
    }

    private Collection<WritableResult> getActionsForUpdate(JoinQueryIndex joinQueryIndex, DocumentMessage message) {
        joinQueryIndex.putDocument(message.getDocument(), message.getDocumentLevel());

        List<Document> outerDocs = joinQueryIndex.getEffectedDocumentsOuter();
        List<Document> innerDocs = joinQueryIndex.getEffectedDocumentsInner();
        joinQueryIndex.refresh();

        return getDenormalizationForDocuments(outerDocs, innerDocs, message.getQuery());

//        return outerDocs
//                .stream()
//                .map(outerDoc -> {
//                    if (message.getQuery().getJoin() != null) {
//                        outerDoc = outerDoc.clone();
//                        outerDoc.setDataField(message.getQuery().getJoinAttributeName(), innerDocs);
//                    }
//                    return outerDoc;
//                })
//                .map(denormDoc -> new WritableResult(denormDoc, message.getQuery().getMurmur3HashAsString(),
//                        WritableResult.Action.INDEX))
//                .collect(Collectors.toSet());
    }

    @Override
    public Collection<WritableResult> apply(DocumentMessage message) {

        // If a join does not exist, we can immediately just return the document as a WritableResult.
        // We do not even need to store the document in the index, as it will only be needed when the document itself
        // is being processed.
        // i.e. We won't need it because it will always be in the DocumentMessage parameter
        if (message.getQuery().getJoin() == null) {
            return ImmutableSet.of(new WritableResult(message.getDocument(), message.getQuery().getMurmur3HashAsString(),
                    WritableResult.Action.INDEX));
        }

        JoinQueryIndex joinQueryIndex = MoreObjects.firstNonNull(_store.get(message.getPartitionKey()), new JoinQueryIndex());
        Collection<WritableResult> writableResults = message.getOpCode() == OpCode.UPDATE ?
                getActionsForUpdate(joinQueryIndex, message) : getActionsForRemove(joinQueryIndex, message);

        joinQueryIndex.refresh();
        _store.put(message.getPartitionKey(), joinQueryIndex);
        return writableResults;

    }
}
