package org.ezstack.denormalizer.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.ezstack.ezapp.datastore.api.Document;

public class WritableResult {

    public enum Action {
        INDEX,
        DELETE
    }

    private final Document _document;
    private final String _table;
    private final Action _action;

    @JsonCreator
    public WritableResult(@JsonProperty("document") Document document,
                          @JsonProperty("table") String table,
                          @JsonProperty("action") Action action) {
        _document = document;
        _table = table;
        _action = action;
    }

    public Document getDocument() {
        return _document;
    }

    public String getTable() {
        return _table;
    }

    public Action getAction() {
        return _action;
    }
}
