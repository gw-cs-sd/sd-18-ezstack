package org.ezstack.denormalizer.model;

import org.ezstack.ezapp.datastore.api.Document;

public class DocumentChangePair {

    private final Document _oldDocument;
    private final Document _newDocument;

    public DocumentChangePair(Document oldDocument, Document newDocument) {
        _oldDocument = oldDocument;
        _newDocument = newDocument;
    }

    public Document getOldDocument() {
        return _oldDocument;
    }

    public Document getNewDocument() {
        return _newDocument;
    }
}
