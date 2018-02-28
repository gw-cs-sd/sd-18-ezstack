package org.ezstack.ezapp.datastore.api;


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

    public void computeDocument(Document doc) {
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
                    case LONG: // fall through
                        return ((double) _longResult)/_documentCount;
                    case DOUBLE:
                        return ((double) _doubleResult)/_documentCount;
                }
        }

        return 0; // unrecognized
    }

    private void count(Document doc) {
        if (doc.containsKey(_searchType.getAttributeOn())) {
            _documentCount++;
        }
    }

    private void max(Document doc) {
        Object value = doc.getValue(_searchType.getAttributeOn());
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
        } else if (_jsonType == DataType.JsonTypes.LONG) {
            long val = (long) value;
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

    private void min(Document doc) {
        Object value = doc.getValue(_searchType.getAttributeOn());
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
        } else if (_jsonType == DataType.JsonTypes.LONG) {
            long val = (long) value;
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

    private void sum(Document doc) {
        Object value = doc.getValue(_searchType.getAttributeOn());
        if (isValidAggregation(value) == null) {
            return;
        }

        count(doc);
        detectAndChangeTypeValue(value);

        if (_jsonType == DataType.JsonTypes.INTEGER) {
            long val = (int) value;
            _longResult += val;
        } else if (_jsonType == DataType.JsonTypes.LONG) {
            long val = (long) value;
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
        DataType.JsonTypes type = DataType.getDataType(value);

        if (_jsonType == DataType.JsonTypes.UNKNOWN) {
            switch (type) {
                case DOUBLE:
                    _jsonType = DataType.JsonTypes.DOUBLE;
                    break;
                case INTEGER:
                    _jsonType = DataType.JsonTypes.INTEGER;
                    break;
                case LONG:
                    _jsonType = DataType.JsonTypes.LONG;
                    break;
            }
        } else if ((_jsonType == DataType.JsonTypes.INTEGER || _jsonType == DataType.JsonTypes.LONG) &&
                type == DataType.JsonTypes.DOUBLE) {
            _doubleResult = _longResult + 0.0;
            _longResult = 0;
            _jsonType = DataType.JsonTypes.DOUBLE;
        } else if ((_jsonType == DataType.JsonTypes.INTEGER && type == DataType.JsonTypes.LONG) ||
                (_jsonType == DataType.JsonTypes.LONG && type == DataType.JsonTypes.INTEGER)) {
            _jsonType = type;
        }
    }

    public static Object isValidAggregation(Object agg) {
        switch (DataType.getDataType(agg)) {
            case DOUBLE:
            case LONG:
            case INTEGER: // fall through
                return agg;
            default:
                return null;
        }
    }
}
