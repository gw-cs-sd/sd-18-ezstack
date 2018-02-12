package org.ezstack.denormalizer.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import org.apache.samza.config.Config;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.task.TaskContext;
import org.ezstack.denormalizer.model.Document;
import org.ezstack.ezapp.datastore.api.KeyBuilder;
import org.ezstack.ezapp.datastore.api.Update;

import java.util.Collection;
import java.util.Map;

public class DocumentResolver implements FlatMapFunction<Update, Document> {

    private static final ObjectMapper _mapper = new ObjectMapper();

    private KeyValueStore<String, Map<String, Object>> _store;

    @Override
    public void init(Config config, TaskContext context) {
        _store = (KeyValueStore<String, Map<String, Object>>) context.getStore("document-resolver");
    }

    @Override
    public Collection<Document> apply(Update update) {
        String storeKey = KeyBuilder.hashKey(update.getTable(), update.getKey());
        Document storedDocument = _mapper.convertValue(_store.get(storeKey), Document.class);

        if (storedDocument == null) {
            storedDocument = new Document(update);
            _store.put(storeKey, _mapper.convertValue(storedDocument, Map.class));
            return ImmutableSet.of(storedDocument);
        }

        int versionBeforeUpdate = storedDocument.getVersion();
        storedDocument.addUpdate(update);
        if (storedDocument.getVersion() != versionBeforeUpdate) {
            _store.put(storeKey, _mapper.convertValue(storedDocument, Map.class));
            return ImmutableSet.of(storedDocument);
        }

        return ImmutableSet.of();
    }
}
