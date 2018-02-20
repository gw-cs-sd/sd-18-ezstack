package org.ezstack.denormalizer.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import org.apache.samza.config.Config;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.task.TaskContext;
import org.ezstack.denormalizer.model.Document;
import org.ezstack.denormalizer.model.DocumentChangePair;
import org.ezstack.ezapp.datastore.api.KeyBuilder;
import org.ezstack.ezapp.datastore.api.Update;

import java.util.Collection;
import java.util.Map;

public class DocumentResolver implements FlatMapFunction<Update, DocumentChangePair> {

    private static final ObjectMapper _mapper = new ObjectMapper();

    private final String _storeName;
    private KeyValueStore<String, Document> _store;

    public DocumentResolver(String storeName) {
        _storeName = storeName;
    }

    @Override
    public void init(Config config, TaskContext context) {
        _store = (KeyValueStore<String, Document>) context.getStore(_storeName);
    }

    @Override
    public Collection<DocumentChangePair> apply(Update update) {
        String storeKey = KeyBuilder.hashKey(update.getTable(), update.getKey());
        Document storedDocument = _store.get(storeKey);

        if (storedDocument == null) {
            storedDocument = new Document(update);
            _store.put(storeKey, storedDocument);
            return ImmutableSet.of(new DocumentChangePair(null, storedDocument));
        }

        Document oldDocument = storedDocument.clone();
        storedDocument.addUpdate(update);
        if (storedDocument.getVersion() != oldDocument.getVersion()) {
            _store.put(storeKey, storedDocument);
            return ImmutableSet.of(new DocumentChangePair(oldDocument, storedDocument));
        }

        return ImmutableSet.of();
    }
}
