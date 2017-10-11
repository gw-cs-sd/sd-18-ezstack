package org.ezstack.ezapp.web;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import org.ezstack.ezapp.writer.WriterConfiguration;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class EZConfiguration extends Configuration {

    @JsonProperty("port")
    private int _port = 8080;

    @Valid
    @NotNull
    @JsonProperty("dataWriter")
    private WriterConfiguration _writerConfiguration;

    public WriterConfiguration getWriterConfiguration() {
        return _writerConfiguration;
    }
}
