package org.ezstack.ezapp.datastore.api;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class MatchAttribute {
    @Valid
    @NotNull
    @JsonProperty("outerAttribute")
    private String outerAttribute;

    @Valid
    @NotNull
    @JsonProperty("innerAttribute")
    private String innerAttribute;
}
