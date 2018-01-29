package org.ezstack.ezapp.datastore.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SearchType {
    public enum Type {
        COUNT, MAX, MIN, SUM, AVERAGE, SEARCH, UNKOWN;

        @Override
        public String toString() {
            switch (this) {
                case COUNT:
                    return "count";
                case MAX:
                    return "max";
                case MIN:
                    return "min";
                case SUM:
                    return "sum";
                case AVERAGE:
                    return "average";
                case SEARCH:
                    return "search";
                default:
                    return "unkown";
            }
        }
    }
    private String _type;
    private String _attributeOn;

    @JsonCreator
    public SearchType(@JsonProperty("type") String type,
                      @JsonProperty("attributeOn") String attributeOn) {
        _type = type;
        _attributeOn = attributeOn;
    }

    public String getAttributeOn() {
        return _attributeOn;
    }

    public Type getType() {
        switch (_type) {
            case "count":
                return Type.COUNT;
            case "max":
                return Type.MAX;
            case "min":
                return Type.MIN;
            case "sum":
                return Type.SUM;
            case "average":
                return Type.AVERAGE;
            case "search":
                return Type.SEARCH;
            default:
                return Type.UNKOWN;
        }
    }
}
