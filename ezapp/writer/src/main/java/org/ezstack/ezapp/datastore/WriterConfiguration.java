package org.ezstack.ezapp.datastore;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.NotEmpty;

public class WriterConfiguration {

    @NotEmpty
    @JsonProperty("bootstrapServers")
    private String _bootstrapServers;

    @NotEmpty
    @JsonProperty("producerName")
    private String _producerName;

    public String getBootstrapServers() {
        return _bootstrapServers;
    }

    public String getProducerName() {
        return _producerName;
    }
}
