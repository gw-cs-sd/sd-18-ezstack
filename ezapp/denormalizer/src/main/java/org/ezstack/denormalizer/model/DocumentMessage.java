package org.ezstack.denormalizer.model;

public class DocumentMessage {

    private final Document _document;
    private final String _partitionKey;

    public DocumentMessage(Document document, String partitionKey) {
        _document = document;
        _partitionKey = partitionKey;
    }

    public Document getDocument() {
        return _document;
    }

    public String getPartitionKey() {
        return _partitionKey;
    }
}
