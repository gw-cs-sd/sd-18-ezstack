package org.ezstack.ezapp.datastore.api;

import java.util.Map;

public class SearchTypeAggregationHelper {
    private SearchType _searchType;
    private long _documentCount;
    private long _longResult;
    private double _doubleResult;
    private DataType.JsonTypes _jsonType;

    public SearchTypeAggregationHelper(SearchType searchType) {
        _searchType = searchType;
        _documentCount = 0;
        _longResult = 0;
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
                sum(doc); // resultVariable/documentCount upon retrieving result
                break;
        }
    }

    public SearchType getSearchType() {
        return _searchType;
    }

    public Object getResult() {
        switch (_searchType.getType()) {
            case COUNT:
                return _documentCount;
            case MAX:
            case MIN:
            case SUM:
                switch (_jsonType) {
                    case INTEGER:
                        return _longResult;
                    case DOUBLE:
                        return _doubleResult;
                }
            case AVG:
                switch (_jsonType) {
                    case INTEGER:
                        return ((double) _longResult)/_documentCount;
                    case DOUBLE:
                        return ((double) _doubleResult)/_documentCount;
                }
        }

        return 0; // unrecognized
    }

    private void count(Map<String, Object> doc) {
        if (doc.containsKey(_searchType.getAttributeOn())) {
            _documentCount++;
        }
    }

    private void max(Map<String, Object> doc) {
        Object value = doc.get(_searchType.getAttributeOn());
        if (isValidAggregation(value) == null) {
            return;
        }

        count(doc);
        detectAndChangeTypeValue(value);

        if (_jsonType == DataType.JsonTypes.INTEGER) {
            long val = (int) value;
            if (_longResult < val || _documentCount == 1) {
                _longResult = val;
            }
        } else if (_jsonType == DataType.JsonTypes.DOUBLE) {
            double val = (double) value;
            if (_doubleResult < val || _documentCount == 1) {
                _doubleResult = val;
            }
        }
    }

    private void min(Map<String, Object> doc) {
        Object value = doc.get(_searchType.getAttributeOn());
        if (isValidAggregation(value) == null) {
            return;
        }

        count(doc);
        detectAndChangeTypeValue(value);

        if (_jsonType == DataType.JsonTypes.INTEGER) {
            long val = (int) value;
            if (_longResult > val || _documentCount == 1) {
                _longResult = val;
            }
        } else if (_jsonType == DataType.JsonTypes.DOUBLE) {
            double val = (double) value;
            if (_doubleResult > val || _documentCount == 1) {
                _doubleResult = val;
            }
        }
    }

    private void sum(Map<String, Object> doc) {
        Object value = doc.get(_searchType.getAttributeOn());
        if (isValidAggregation(value) == null) {
            return;
        }

        count(doc);
        detectAndChangeTypeValue(value);

        if (_jsonType == DataType.JsonTypes.INTEGER) {
            long val = (int) value;
            _longResult += val;
        } else if (_jsonType == DataType.JsonTypes.DOUBLE) {
            double val = (double) value;
            _doubleResult += val;
        }
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
            _doubleResult = _longResult + 0.0;
            _longResult = 0;
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
