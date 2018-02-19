package org.ezstack.denormalizer.core;

import com.google.common.collect.ImmutableSet;
import org.apache.samza.config.Config;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.task.TaskContext;
import org.ezstack.denormalizer.model.Document;
import org.ezstack.denormalizer.model.DocumentMessage;
import org.ezstack.denormalizer.model.JoinQueryIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

public class DocumentJoiner implements FlatMapFunction<DocumentMessage, Document> {

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

    @Override
    public Collection<Document> apply(DocumentMessage message) {
        return ImmutableSet.of();

    }
}
