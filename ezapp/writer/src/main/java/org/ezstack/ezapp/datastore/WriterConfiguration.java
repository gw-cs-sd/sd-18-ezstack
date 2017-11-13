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

    @NotEmpty
    @JsonProperty("writerTopicName")
    private String _writerTopicName;

    public String getBootstrapServers() {
        return _bootstrapServers;
    }

    public String getProducerName() {
        return _producerName;
    }

    public String getWriterTopicName() {
        return _writerTopicName;
    }
}
