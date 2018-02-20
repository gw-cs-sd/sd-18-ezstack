package org.ezstack.denormalizer.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.ezstack.ezapp.datastore.api.Query;

public class DocumentMessage {

    public enum OpCode {
        UPDATE,
        DELETE
    }


    private final Document _document;
    private final String _partitionKey;
    private final QueryLevel _level;
    private final OpCode _opCode;
    private final Query _query;

    @JsonCreator
    public DocumentMessage(@JsonProperty("document") Document document,
                           @JsonProperty("partitionKey") String partitionKey,
                           @JsonProperty("documentLevel") QueryLevel level,
                           @JsonProperty("opCode") OpCode opCode,
                           @JsonProperty("query") Query query) {
        _document = document;
        _partitionKey = partitionKey;
        _level = level;
        _opCode = opCode;
        _query = query;
    }

    public Document getDocument() {
        return _document;
    }

    public String getPartitionKey() {
        return _partitionKey;
    }

    public QueryLevel getDocumentLevel() {
        return _level;
    }

    public OpCode getOpCode() {
        return _opCode;
    }

    public Query getQuery() {
        return _query;
    }
}
