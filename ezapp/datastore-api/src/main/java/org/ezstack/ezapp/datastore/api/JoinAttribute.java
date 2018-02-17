package org.ezstack.ezapp.datastore.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Charsets;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;

import javax.validation.constraints.NotNull;

/**
 * The following class is used to
 */
public class JoinAttribute {
    private String _outerAttribute;

    private String _innerAttribute;

    @JsonCreator
    public JoinAttribute(@NotNull @JsonProperty("outerAttribute") String outerAttribute,
                         @NotNull @JsonProperty("innerAttribute") String innerAttribute) {
        _outerAttribute = outerAttribute;
        _innerAttribute = innerAttribute;
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

    public String getMurmur3HashAsString() {
        return getMurmur3Hash().toString();
    }
}
