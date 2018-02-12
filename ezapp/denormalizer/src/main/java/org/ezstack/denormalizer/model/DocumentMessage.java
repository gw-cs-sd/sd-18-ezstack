package org.ezstack.denormalizer.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DocumentMessage {

    private final Document _document;
    private final String _partitionKey;

    @JsonCreator
    public DocumentMessage(@JsonProperty("document") Document document,
                           @JsonProperty("partitionKey") String partitionKey) {
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
