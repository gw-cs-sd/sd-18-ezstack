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
        switch (_searchType.getType()) {
            case COUNT:
                count(doc);
                break;
            case MAX:
                max(doc);
                break;
            case MIN:
                min(doc);
                break;
            case SUM:
                sum(doc);
                break;
            case AVG:
                avg(doc);
                break;
        }
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

    private void count(Map<String, Object> doc) {
        // TODO
    }

    private void max(Map<String, Object> doc) {
        // TODO
    }

    private void min(Map<String, Object> doc) {
        // TODO
    }

    private void sum(Map<String, Object> doc) {
        // TODO
    }

    private void avg(Map<String, Object> doc) {
        // TODO
    }

    /**
     * method detects value type and sets the type appropriately.
     *
     * if for some reason it detects that the type has changed from integer to double then it will
     * migrate the current results onto the double value and clear the integer value out.
     *
     * no such check is done from double to int because we never convert values to a lower precision.
     * @param value
     */
    private void detectAndChangeTypeValue(Object value) {
        if (_jsonType == DataType.JsonTypes.UNKNOWN) {
            switch (DataType.getDataType(value)) {
                case DOUBLE:
                    _jsonType = DataType.JsonTypes.DOUBLE;
                    break;
                case INTEGER:
                    _jsonType = DataType.JsonTypes.INTEGER;
                    break;
            }
        } else if (_jsonType == DataType.JsonTypes.INTEGER &&
                DataType.getDataType(value) == DataType.JsonTypes.DOUBLE) {
            _doubleResult = _intResult + 0.0;
            _intResult = 0;
        }
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
