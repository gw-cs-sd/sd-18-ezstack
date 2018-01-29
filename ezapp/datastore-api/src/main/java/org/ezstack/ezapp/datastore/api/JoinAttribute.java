package org.ezstack.ezapp.datastore.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

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
}
