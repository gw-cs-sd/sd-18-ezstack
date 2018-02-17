package org.ezstack.ezapp.datastore.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import com.google.common.base.Charsets;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;

import javax.validation.constraints.NotNull;


public class Filter {
    public enum Operations {
        EQ, NOT_EQ, GT, GTE, LT, LTE, UNKOWN;

        @Override
        public String toString() {
            switch (this) {
                case EQ:
                    return "eq";
                case NOT_EQ:
                    return "not_eq";
                case GT:
                    return "gt";
                case GTE:
                    return "gte";
                case LT:
                    return "lt";
                case LTE:
                    return "lte";
                default:
                    return "unkown";
            }
        }
    }

    private String _attribute;
    private String _opt;
    private Object _value;

    @JsonCreator
    public Filter(@NotNull @JsonProperty("attribute") String attribute,
                  @NotNull @JsonProperty("opt") String opt,
                  @NotNull @JsonProperty("value") Object value) {
        _attribute = attribute;
        _opt = opt;
        _value = value;
    }

    public Filter(String attribute, Operations opt, Object value) {
        _attribute = attribute;
        _opt = opt.toString();
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

    @JsonIgnore
    public HashCode getMurmur3Hash() {
        StringBuilder sb = new StringBuilder();
        sb.append(getAttribute()).append("~").append(getOpt().toString()).append("~").append(getValue().toString());

        return Hashing.murmur3_128().newHasher()
                .putString(sb.toString(), Charsets.UTF_8)
                .hash();
    }

    public String getMurmur3HashAsString() {
        return getMurmur3Hash().toString();
    }
}
