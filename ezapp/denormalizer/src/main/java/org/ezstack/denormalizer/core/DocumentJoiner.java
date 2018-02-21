package org.ezstack.denormalizer.core;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;
import org.apache.samza.config.Config;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.task.TaskContext;
import org.ezstack.denormalizer.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

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

    private Collection<WritableResult> getActionsForDelete(JoinQueryIndex joinQueryIndex, DocumentMessage message) {
        joinQueryIndex.deleteDocument(message.getDocument(), message.getDocumentLevel());

        List<Document> outerDocs = joinQueryIndex.getEffectedDocumentsOuter();
        List<Document> innerDocs = joinQueryIndex.getEffectedDocumentsInner();
        joinQueryIndex.refresh();
        
        switch (message.getDocumentLevel()) {
            case OUTER:
                return outerDocs
                        .parallelStream()
                        .map(docToDelete -> new WritableResult(docToDelete, message.getQuery().getMurmur3HashAsString(),
                                OpCode.DELETE))
                        .collect(Collectors.toSet());
            case INNER:
                return outerDocs
                    .parallelStream()
                    .map(outerDoc -> {
                        outerDoc = outerDoc.clone();
                        outerDoc.setDataField(message.getQuery().getJoinAttributeName(), innerDocs);
                        return outerDoc;
                    })
                    .map(denormDoc -> new WritableResult(denormDoc, message.getQuery().getMurmur3HashAsString(), OpCode.UPDATE))
                    .collect(Collectors.toSet());
        }

        return ImmutableSet.of();
    }

    private Collection<WritableResult> getActionsForUpdate(JoinQueryIndex joinQueryIndex, DocumentMessage message) {
        joinQueryIndex.putDocument(message.getDocument(), message.getDocumentLevel());

        List<Document> outerDocs = joinQueryIndex.getEffectedDocumentsOuter();
        List<Document> innerDocs = joinQueryIndex.getEffectedDocumentsInner();
        joinQueryIndex.refresh();

        return outerDocs
                .parallelStream()
                .map(outerDoc -> {
                    outerDoc = outerDoc.clone();
                    outerDoc.setDataField(message.getQuery().getJoinAttributeName(), innerDocs);
                    return outerDoc;
                })
                .map(denormDoc -> new WritableResult(denormDoc, message.getQuery().getMurmur3HashAsString(), OpCode.UPDATE))
                .collect(Collectors.toSet());
    }

    @Override
    public Collection<WritableResult> apply(DocumentMessage message) {

        JoinQueryIndex joinQueryIndex = MoreObjects.firstNonNull(_store.get(message.getPartitionKey()), new JoinQueryIndex());
        Collection<WritableResult> writableResults = message.getOpCode() == OpCode.UPDATE ?
                getActionsForUpdate(joinQueryIndex, message) : getActionsForDelete(joinQueryIndex, message);

        joinQueryIndex.refresh();
        _store.put(message.getPartitionKey(), joinQueryIndex);
        return writableResults;

        // TODO: handle aggregations

    }
}
