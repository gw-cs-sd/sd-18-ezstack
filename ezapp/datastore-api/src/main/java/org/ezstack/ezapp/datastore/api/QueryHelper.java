package org.ezstack.ezapp.datastore.api;

import java.util.*;

public class QueryHelper {

    public static Map<String, Object> filterAttributes(List<String> excludeAttributes,
                                                       List<String> includeAttributes,
                                                       Map<String, Object> doc) {
        return includeAttributes == null ?
                excludeAttributes(excludeAttributes, doc) : includeAttributes(includeAttributes, doc);
    }

    public static List<SearchTypeAggregationHelper> createAggHelpers(List<SearchType> searchTypeList) {
        searchTypeList = safe(searchTypeList);
        List<SearchTypeAggregationHelper> ret = new LinkedList<>();

        for (SearchType st: searchTypeList) {
            if (st.getType() != SearchType.Type.SEARCH) {
                ret.add(new SearchTypeAggregationHelper(st));
            }
        }

        return ret;
    }

    public static void updateAggHelpers(List<SearchTypeAggregationHelper> aggregationHelpers, Map<String, Object> doc) {
        aggregationHelpers = safe(aggregationHelpers);

        for (SearchTypeAggregationHelper helper: aggregationHelpers) {
            helper.computeDocument(doc);
        }
    }

    public static boolean hasSearchRequest(List<SearchType> searchTypeList) {
        searchTypeList = safe(searchTypeList);

        for (SearchType st: searchTypeList) {
            if (st.getType() == SearchType.Type.SEARCH) {
                return true;
            }
        }

        return false;
    }

    /**
     * @param filters
     * @param doc
     * @return true if document passes all filters, otherwise false
     */
    public static boolean meetsFilters(List<Filter> filters, Map<String, Object> doc) {
        // TODO
        return false;
    }

    public static List safe(List l) {
        return l == null ? Collections.emptyList() : l;
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
}
