package org.ezstack.ezapp.datastore.api;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;


public class Filter {
    enum Operations {
        EQ, NOT_EQ, GT, GTE, LT, LTE, UNKOWN
    }

    @NotNull
    @JsonProperty("attribute")
    private String attribute;

    @NotNull
    @JsonProperty("opt")
    private String opt;

    @NotNull
    @JsonProperty("value")
    private Object value;

    public String getAttribute() {
        return attribute;
    }

    public Operations getOpt() {
        switch (opt.toLowerCase()) {
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
        return value;
    }
}
