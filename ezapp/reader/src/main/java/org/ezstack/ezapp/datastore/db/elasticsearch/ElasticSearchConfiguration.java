package org.ezstack.ezapp.datastore.db.elasticsearch;

import com.fasterxml.jackson.annotation.JsonProperty;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;


public class ElasticSearchConfiguration {

    @NotNull
    @JsonProperty("clusterName")
    private String _clusterName = "elasticsearch";

    @NotNull
    @JsonProperty("transportAddresses")
    private List<Map<String, Integer>> _transportAddresses;

    public String getClusterName() {
        return _clusterName;
    }

    public List<Map<String, Integer>> getTransportAddresses() {
        return _transportAddresses;
    }
}
