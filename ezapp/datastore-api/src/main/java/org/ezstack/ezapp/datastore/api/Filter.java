package org.ezstack.ezapp.datastore.api;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;


public class Filter {
    enum Operations {
        EQ, NOT_EQ, GT, GTE, LT, LTE, UNKOWN
    }

    @NotNull
    @JsonProperty("attribute")
    private String _attribute;

    @NotNull
    @JsonProperty("opt")
    private String _opt;

    @NotNull
    @JsonProperty("value")
    private Object _value;

    /**
     * Empty constructor for serialization
     */
    public Filter() {
    }

    public Filter(String attribute, String opt, Object value) {
        _attribute = attribute;
        _opt = opt;
        _value = value;
    }

    public String getAttribute() {
        return _attribute;
    }

    public Operations getOpt() {
        switch (_opt.toLowerCase()) {
            case "eq":
            case "==": // fall through
                return Operations.EQ;
            case "not_eq":
            case "!=": // fall through
                return Operations.NOT_EQ;
            case "gt":
            case ">": // fall through
                return Operations.GT;
            case "gte":
            case ">=": // fall through
                return Operations.GTE;
            case "lt":
            case "<": // fall through
                return Operations.LT;
            case "lte":
            case "<=": // fall through
                return Operations.LTE;
            default:
                return Operations.UNKOWN;
        }
    }

    public Object getValue() {
        return _value;
    }
}
