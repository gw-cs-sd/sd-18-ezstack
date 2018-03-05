package org.ezstack.ezapp.datastore.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Charsets;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;

import static com.google.common.base.Preconditions.checkNotNull;

public class SearchType {
    public enum Type {
        COUNT, MAX, MIN, SUM, AVG, SEARCH;

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
                    return "unknown";
            }
        }
    }

    private final Type _type;
    private final String _attributeOn;

    @JsonCreator
    public SearchType(@JsonProperty("type") String type,
                      @JsonProperty("attributeOn") String attributeOn) {

        switch (type.toLowerCase()) {
            case "count":
                _type = Type.COUNT;
                break;
            case "max":
                _type = Type.MAX;
                break;
            case "min":
                _type = Type.MIN;
                break;
            case "sum":
                _type = Type.SUM;
                break;
            case "avg":
                _type = Type.AVG;
                break;
            case "search":
                _type = Type.SEARCH;
                break;
            default:
                _type = null;
                break;

        }

        checkNotNull(_type);

        _attributeOn = attributeOn;
    }

    public String getAttributeOn() {
        return _attributeOn;
    }

    public Type getType() {
        return _type;
    }

    @Override
    public String toString() {
        return "_" + getType().toString() + "_" + _attributeOn;
    }

    @JsonIgnore
    public HashCode getMurmur3Hash() {
        return Hashing.murmur3_128().newHasher()
                .putString(toString(), Charsets.UTF_8)
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

        SearchType that = (SearchType) o;

        if (getType() != that.getType()) return false;
        return _attributeOn != null ? _attributeOn.equals(that._attributeOn) : that._attributeOn == null;
    }

    @Override
    public int hashCode() {
        int result = getType().toString().hashCode();
        result = 31 * result + (_attributeOn != null ? _attributeOn.hashCode() : 0);
        return result;
    }
}
