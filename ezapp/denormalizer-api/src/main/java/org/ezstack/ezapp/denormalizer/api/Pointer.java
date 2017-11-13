package org.ezstack.ezapp.denormalizer.api;

public class Pointer {
    private String _table;
    private String _id;

    public Pointer(String table, String id) {
        _table = table;
        _id = id;
    }

    public String getTable() {
        return _table;
    }

    public String getId() {
        return _id;
    }
}
