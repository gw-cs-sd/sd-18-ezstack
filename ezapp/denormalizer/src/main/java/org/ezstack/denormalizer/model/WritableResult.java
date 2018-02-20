package org.ezstack.denormalizer.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class WritableResult {

    private final Document _document;
    private final String _table;

    @JsonCreator
    public WritableResult(@JsonProperty("document") Document document,
                          @JsonProperty("table") String table) {
        _document = document;
        _table = table;
    }

    public Document getDocument() {
        return _document;
    }

    public String getTable() {
        return _table;
    }
}
