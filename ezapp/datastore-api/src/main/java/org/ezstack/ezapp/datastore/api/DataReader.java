package org.ezstack.ezapp.datastore.api;

import java.util.List;
import java.util.Map;

public interface DataReader {
    Map<String, Object> getDocument(String database, String table, String id);

    List<Map<String, Object>> getDocuments(Query query);
}
