package org.ezstack.ezapp.web;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import org.ezstack.ezapp.datastore.WriterConfiguration;
import org.ezstack.ezapp.datastore.db.elasticsearch.ElasticSearchConfiguration;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class EZConfiguration extends Configuration {

    @JsonProperty("port")
    private int _port = 8080;

    @Valid
    @NotNull
    @JsonProperty("dataWriter")
    private WriterConfiguration _writerConfiguration;

    @Valid
    @NotNull
    @JsonProperty("elasticConfiguration")
    private ElasticSearchConfiguration _elasticSearchConfiguration = new ElasticSearchConfiguration();

    public WriterConfiguration getWriterConfiguration() {
        return _writerConfiguration;
    }

    public ElasticSearchConfiguration getElasticSearchConfiguration() {
        return _elasticSearchConfiguration;
    }
}
