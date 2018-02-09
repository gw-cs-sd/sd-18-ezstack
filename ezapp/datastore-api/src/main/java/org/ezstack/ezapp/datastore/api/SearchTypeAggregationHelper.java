package org.ezstack.ezapp.datastore.api;

import java.util.Map;

public class SearchTypeAggregationHelper {
    private SearchType _searchType;
    private int _documentCount;
    private int _intResult;
    private double _doubleResult;
    private DataType.JsonTypes _jsonType;

    public SearchTypeAggregationHelper(SearchType searchType) {
        _searchType = searchType;
        _documentCount = 0;
        _intResult = 0;
        _doubleResult = 0.0;
        _jsonType = DataType.JsonTypes.UNKNOWN;
    }

    public void computeDocument(Map<String, Object> doc) {
        // TODO
    }

    public Object getResult() {
        switch (_jsonType) {
            case INTEGER:
                return _intResult;
            case DOUBLE:
                return _doubleResult;
            default: // not a valid agg
                return 0;
        }
    }

    private void detectAndChangeTypeValue(Object value) {
        // TODO
    }

    public static Object isValidAggregation(Object agg) {
        switch (DataType.getDataType(agg)) {
            case DOUBLE:
            case INTEGER: // fall through
                return agg;
            default:
                return null;
        }
    }
}
