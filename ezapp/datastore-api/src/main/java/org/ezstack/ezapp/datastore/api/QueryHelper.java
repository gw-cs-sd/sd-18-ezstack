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
        filters = safe(filters);
        for (Filter f: filters) {
            if (!meetsFilter(f, doc)) return false;
        }
        return true;
    }

    public static boolean meetsFilter(Filter f, Map<String, Object> doc) {
        Object val = doc.get(f.getAttribute());
        if (val == null) {
            return false;
        }

        switch (f.getOpt()) {
            case EQ:
                return val.equals(f.getValue());
            case NOT_EQ:
                return !val.equals(f.getValue());
            case GT:
                return compare(val, f.getValue(), Filter.Operations.GT);
            case GTE:
                return compare(val, f.getValue(), Filter.Operations.GTE);
            case LT:
                return compare(val, f.getValue(), Filter.Operations.LT);
            case LTE:
                return compare(val, f.getValue(), Filter.Operations.LTE);
        }
        return false;
    }

    private static boolean compare(Object o1, Object o2, Filter.Operations op) {
        DataType.JsonTypes type = DataType.getDataType(o1);
        if (type != DataType.getDataType(o2) || type == DataType.JsonTypes.UNKNOWN ||
                // should we have some fun with list and map comparisons ?
                type == DataType.JsonTypes.LIST || type == DataType.JsonTypes.MAP) {
            return false;
        }

        switch (type) {
            case INTEGER:
                int o1Int = (int) o1;
                int o2Int = (int) o2;
                switch (op) {
                    case GT:
                        return o1Int > o2Int;
                    case GTE:
                        return o1Int >= o2Int;
                    case LT:
                        return o1Int < o2Int;
                    case LTE:
                        return o1Int <= o2Int;
                }
                break;
            case LONG:
                long o1Long = (long) o1;
                long o2Long = (long) o2;
                switch (op) {
                    case GT:
                        return o1Long > o2Long;
                    case GTE:
                        return o1Long >= o2Long;
                    case LT:
                        return o1Long < o2Long;
                    case LTE:
                        return o1Long <= o2Long;
                }
                break;
            case DOUBLE:
                double o1Double = (double) o1;
                double o2Double = (double) o2;
                return o1Double > o2Double;
            case BOOLEAN:
                Boolean o1Bool = (Boolean) o1;
                Boolean o2Bool = (Boolean) o2;
                switch (op) {
                    case GT:
                        return o1Bool.compareTo(o2Bool) > 0;
                    case GTE:
                        return o1Bool.compareTo(o2Bool) >= 0;
                    case LT:
                        return o1Bool.compareTo(o2Bool) < 0;
                    case LTE:
                        return o1Bool.compareTo(o2Bool) <= 0;
                }
                break;
            case STRING:
                String o1Str = (String) o1;
                String o2Str = (String) o2;
                switch (op) {
                    case GT:
                        return o1Str.compareTo(o2Str) > 0;
                    case GTE:
                        return o1Str.compareTo(o2Str) >= 0;
                    case LT:
                        return o1Str.compareTo(o2Str) < 0;
                    case LTE:
                        return o1Str.compareTo(o2Str) <= 0;
                }
                break;
        }

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
