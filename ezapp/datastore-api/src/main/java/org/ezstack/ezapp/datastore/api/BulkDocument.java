package org.ezstack.ezapp.datastore.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;
import java.util.Map;

public class BulkDocument {
    public enum BulkOperation {
        CREATE, UPDATE, UNKOWN
    }

    private String _table;
    private String _key;
    private Map<String, Object> _document;
    private BulkOperation _opType;


    @JsonCreator
    public BulkDocument(@JsonProperty("table") @NotNull String table,
                        @JsonProperty("key") String key,
                        @JsonProperty("document") @NotNull Map<String, Object> document,
                        @JsonProperty("opType") @NotNull String opType) {
        _table = table;
        _key = key;
        _document = document;
        _opType = getOperation(opType);
    }

    public String getTable() {
        return _table;
    }

    public String getKey() {
        return _key;
    }

    public Map<String, Object> getDocument() {
        return _document;
    }

    public BulkOperation getOpType() {
        return _opType;
    }

    public static BulkOperation getOperation(String opType) {
        switch (opType.toLowerCase()) {
            case "create":
                return BulkOperation.CREATE;
            case "update":
                return BulkOperation.UPDATE;
            default:
                return BulkOperation.UNKOWN;
        }
    }
}
