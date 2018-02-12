package org.ezstack.ezapp.datastore.db.elasticsearch;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.List;


public class ElasticSearchConfiguration {
    @Valid
    @NotNull
    @JsonProperty("clusterName")
    private String _clusterName = "elasticsearch";

    @Valid
    @JsonProperty("transportAddresses")
    private List<TransportAddressConfig> _transportAddresses;

    public String getClusterName() {
        return _clusterName;
    }

    public List<TransportAddressConfig> getTransportAddresses() {
        return _transportAddresses;
    }
}
