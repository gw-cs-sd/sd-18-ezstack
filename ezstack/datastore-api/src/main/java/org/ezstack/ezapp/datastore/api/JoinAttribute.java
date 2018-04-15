package org.ezstack.ezapp.datastore.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Charsets;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;

import javax.validation.constraints.NotNull;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * The following class is used to
 */
public class JoinAttribute {
    private final String _outerAttribute;

    private final String _innerAttribute;

    @JsonCreator
    public JoinAttribute(@NotNull @JsonProperty("outerAttribute") String outerAttribute,
                         @NotNull @JsonProperty("innerAttribute") String innerAttribute) {
        _outerAttribute = checkNotNull(outerAttribute);
        _innerAttribute = checkNotNull(innerAttribute);
    }

    public String getOuterAttribute() {
        return _outerAttribute;
    }

    public String getInnerAttribute() {
        return _innerAttribute;
    }

    @JsonIgnore
    public HashCode getMurmur3Hash() {
        StringBuilder sb = new StringBuilder();
        sb.append(getOuterAttribute()).append("~").append(getInnerAttribute());

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

        JoinAttribute that = (JoinAttribute) o;

        if (_outerAttribute != null ? !_outerAttribute.equals(that._outerAttribute) : that._outerAttribute != null)
            return false;
        return _innerAttribute != null ? _innerAttribute.equals(that._innerAttribute) : that._innerAttribute == null;
    }

    @Override
    public int hashCode() {
        int result = _outerAttribute != null ? _outerAttribute.hashCode() : 0;
        result = 31 * result + (_innerAttribute != null ? _innerAttribute.hashCode() : 0);
        return result;
    }
}
