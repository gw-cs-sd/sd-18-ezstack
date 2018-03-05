package org.ezstack.ezapp.datastore.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import com.google.common.base.Charsets;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;

import javax.validation.constraints.NotNull;

import static com.google.common.base.Preconditions.checkNotNull;


public class Filter {
    public enum Operation {
        EQ, NOT_EQ, GT, GTE, LT, LTE;

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
                    return "unknown";
            }
        }
    }

    private String _attribute;
    private Operation _op;
    private Object _value;

    @JsonCreator
    public Filter(@NotNull @JsonProperty("attribute") String attribute,
                  @NotNull @JsonProperty("op") String op,
                  @NotNull @JsonProperty("value") Object value) {
        switch (op.toLowerCase()) {
            case "eq":
            case "==": // fall through
                _op = Operation.EQ;
                break;
            case "not_eq":
            case "!=": // fall through
                _op = Operation.NOT_EQ;
                break;
            case "gt":
            case ">": // fall through
                _op =  Operation.GT;
                break;
            case "gte":
            case ">=": // fall through
                _op = Operation.GTE;
                break;
            case "lt":
            case "<": // fall through
                _op = Operation.LT;
                break;
            case "lte":
            case "<=": // fall through
                _op = Operation.LTE;
                break;
            default:
                _op = null;
                break;
        }

        checkNotNull(_op);
        _attribute = attribute;
        _value = value;
    }

    public Filter(String attribute, Operation op, Object value) {
        _attribute = attribute;
        _op = op;
        _value = value;
    }

    public String getAttribute() {
        return _attribute;
    }

    public Operation getOp() {
        return _op;
    }

    public Object getValue() {
        return _value;
    }

    @JsonIgnore
    public HashCode getMurmur3Hash() {
        StringBuilder sb = new StringBuilder();
        sb.append(getAttribute()).append("~").append(getOp().toString()).append("~").append(getValue().toString());

        return Hashing.murmur3_128().newHasher()
                .putString(sb.toString(), Charsets.UTF_8)
                .hash();
    }

    @JsonIgnore
    public String getMurmur3HashAsString() {
        return getMurmur3Hash().toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Filter filter = (Filter) o;

        if (_attribute != null ? !_attribute.equals(filter._attribute) : filter._attribute != null) return false;
        if (getOp() != filter.getOp()) return false;
        return _value != null ? _value.equals(filter._value) : filter._value == null;
    }

    @Override
    public int hashCode() {
        int result = _attribute != null ? _attribute.hashCode() : 0;
        result = 31 * result + getOp().toString().hashCode();
        result = 31 * result + (_value != null ? _value.hashCode() : 0);
        return result;
    }
}