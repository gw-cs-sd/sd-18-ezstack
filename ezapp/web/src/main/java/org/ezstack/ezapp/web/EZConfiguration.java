package org.ezstack.ezapp.web;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import org.ezstack.ezapp.datastore.WriterConfiguration;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class EZConfiguration extends Configuration {

    @Valid
    @NotNull
    @JsonProperty("dataWriter")
    private WriterConfiguration _writerConfiguration;

    @Valid
    @NotNull
    @JsonProperty("zookeeperHosts")
    private String _zookeeperHosts;

    public WriterConfiguration getWriterConfiguration() {
        return _writerConfiguration;
    }

    public String getZookeeperHosts() {
        return _zookeeperHosts;
    }
}
