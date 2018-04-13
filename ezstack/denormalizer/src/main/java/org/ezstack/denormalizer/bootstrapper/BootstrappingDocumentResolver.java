package org.ezstack.denormalizer.bootstrapper;

import org.apache.samza.config.Config;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.task.TaskContext;
import org.ezstack.denormalizer.model.DocumentChangePair;
import org.ezstack.ezapp.datastore.api.Document;
import org.ezstack.ezapp.datastore.api.KeyBuilder;

public class BootstrappingDocumentResolver implements MapFunction<Document, DocumentChangePair> {

    private final String _storeName;
    private KeyValueStore<String, Document> _store;

    public BootstrappingDocumentResolver(String storeName) {
        _storeName = storeName;
    }

    @Override
    public void init(Config config, TaskContext context) {
        _store = (KeyValueStore<String, Document>) context.getStore(_storeName);
    }

    @Override
    public DocumentChangePair apply(Document document) {
        String storeKey = KeyBuilder.hashKey(document.getTable(), document.getKey());
        Document storedDocument = _store.get(storeKey);
        _store.put(storeKey, document);
        return new DocumentChangePair(storedDocument, document);
    }
}
