package org.ezstack.ezapp.datastore.api;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryHelper {

    public static Map<String, Object> filterAttributes(List<String> excludeAttributes,
                                                       List<String> includeAttributes,
                                                       Map<String, Object> doc) {
        return includeAttributes == null ?
                excludeAttributes(excludeAttributes, doc) : includeAttributes(includeAttributes, doc);
    }

    private static Map<String, Object> excludeAttributes(List<String> excludeAttributes, Map<String, Object> doc) {
        if (excludeAttributes == null || excludeAttributes.size() == 0) {
            return doc;
        }

        for (String attribute: excludeAttributes) {
            doc.remove(attribute);
        }
        return doc;
    }

    private static Map<String, Object> includeAttributes(List<String> includeAttributes, Map<String, Object> doc) {
        if (includeAttributes == null || includeAttributes.size() == 0) {
            return doc;
        }

        Map<String, Object> newDoc = new HashMap<>();
        for (String attribute: includeAttributes) {
            Object value = doc.get(attribute);
            if (value != null) {
                newDoc.put(attribute, value);
            }
        }
        return newDoc;
    }

    /**
     * @param query
     * @param doc
     * @return true if document passes all filters, otherwise false
     */
    public static boolean meetsFilters(Query query, Map<String, Object> doc) {
        // TODO
        return false;
    }
}
