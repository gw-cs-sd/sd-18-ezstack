package org.ezstack.ezapp.datastore.api;

import java.util.List;
import java.util.Map;

public class QueryHelper {

    public static QueryResult generateQueryResult(Query query, List<Map<String, Object>> documents) {
        // TODO
        return null;
    }

    /**
     * Modifies the document passed as well as returining it
     * @param query
     * @param document
     * @return
     */
    public static Map<String, Object> removeUnWantedAttributes(Query query, Map<String, Object> document) {
        // TODO
        return null;
    }

    /**
     * @param query
     * @param document
     * @return true if document passes all filters, otherwise false
     */
    public static boolean meetsFilters(Query query, Map<String, Object> document) {
        // TODO
        return false;
    }
}
