package org.ezstack.denormalizer.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.ezstack.ezapp.datastore.api.Document;

public class WritableResult {

    private final Document _document;
    private final String _table;
    private final OpCode _opCode;

    @JsonCreator
    public WritableResult(@JsonProperty("document") Document document,
                          @JsonProperty("table") String table,
                          @JsonProperty("opCode") OpCode opCode) {
        _document = document;
        _table = table;
        _opCode = opCode;
    }

    public Document getDocument() {
        return _document;
    }

    public String getTable() {
        return _table;
    }

    public OpCode getOpCode() {
        return _opCode;
    }
}
