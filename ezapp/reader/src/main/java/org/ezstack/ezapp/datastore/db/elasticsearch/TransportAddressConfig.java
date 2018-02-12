package org.ezstack.ezapp.datastore.db.elasticsearch;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class TransportAddressConfig {
    @Valid
    @NotNull
    @JsonProperty("address")
    private String _address;

    @Valid
    @NotNull
    @JsonProperty("port")
    private int _port;

    public String getAddress() {
        return _address;
    }

    public int getPort() {
        return _port;
    }
}