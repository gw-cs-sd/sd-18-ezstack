package org.ezstack.ezapp.datastore.api;

import java.util.Map;

public interface DataWriter {

    /**
     * create document for given table and key
     * @return document key
     */
    String create(String table, String key, Map<String, Object> document);

    /**
     * create document for given table with random key
     * @return document key
     */
    String create(String table, Map<String, Object> document);

    /**
     * update for given table and key
     * @return document key
     */
    String update(String table, String key, Map<String, Object> update);
}
