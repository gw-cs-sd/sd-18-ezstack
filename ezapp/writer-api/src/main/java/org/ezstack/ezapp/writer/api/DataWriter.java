package org.ezstack.ezapp.writer.api;

import java.util.Map;

public interface DataWriter {

    void create(String table, String key, Map<String, Object> document);
}
