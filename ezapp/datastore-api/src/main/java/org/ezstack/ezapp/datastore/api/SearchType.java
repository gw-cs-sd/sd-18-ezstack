package org.ezstack.ezapp.datastore.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SearchType {
    public enum Type {
        COUNT, MAX, MIN, SUM, AVG, SEARCH, UNKOWN;

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
                case AVG:
                    return "avg";
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
            case "avg":
                return Type.AVG;
            case "search":
                return Type.SEARCH;
            default:
                return Type.UNKOWN;
        }
    }

    @Override
    public String toString() {
        return "_" + getType().toString() + "_" + _attributeOn;
    }
}
