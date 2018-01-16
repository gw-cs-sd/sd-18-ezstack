package org.ezstack.ezapp.datastore.api;

import java.util.Map;

public interface DataWriter {

    void create(String table, String key, Map<String, Object> document);

    void update(String table, String key, Map<String, Object> update);
}
