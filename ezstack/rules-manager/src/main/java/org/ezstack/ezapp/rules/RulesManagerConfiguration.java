package org.ezstack.ezapp.rules;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.NotEmpty;

public class RulesManagerConfiguration {

    @NotEmpty
    @JsonProperty("zookeeperHosts")
    private String _zookeeperHosts;

    public String getZookeeperHosts() {
        return _zookeeperHosts;
    }
}