package org.ezstack.denormalizer.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DocumentMessage {

    public enum DocumentLevel {
        OUTER,
        INNER
    }

    public enum OpCode {
        UPDATE,
        DELETE
    }


    private final Document _document;
    private final String _partitionKey;
    private final DocumentLevel _level;
    private final OpCode _opCode;

    @JsonCreator
    public DocumentMessage(@JsonProperty("document") Document document,
                           @JsonProperty("partitionKey") String partitionKey,
                           @JsonProperty("documentLevel") DocumentLevel level,
                           @JsonProperty("opCode") OpCode opCode) {
        _document = document;
        _partitionKey = partitionKey;
        _level = level;
        _opCode = opCode;
    }

    public Document getDocument() {
        return _document;
    }

    public String getPartitionKey() {
        return _partitionKey;
    }

    public DocumentLevel getDocumentLevel() {
        return _level;
    }

    public OpCode getOpCode() {
        return _opCode;
    }
}
