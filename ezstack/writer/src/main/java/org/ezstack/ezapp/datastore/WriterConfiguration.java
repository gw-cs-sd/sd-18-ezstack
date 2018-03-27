package org.ezstack.ezapp.datastore;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.NotNull;

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

    @NotNull
    @JsonProperty("writerTopicPartitionCount")
    private Integer _writerTopicPartitionCount;

    @NotEmpty
    @JsonProperty("zookeeperHosts")
    private String _zookeeperHosts;

    @NotNull
    @JsonProperty("writerTopicReplicationFactor")
    private Integer _writerTopicReplicationFactor;

    public String getBootstrapServers() {
        return _bootstrapServers;
    }

    public String getProducerName() {
        return _producerName;
    }

    public String getWriterTopicName() {
        return _writerTopicName;
    }

    public int getWriterTopicPartitionCount() {
        return _writerTopicPartitionCount;
    }

    public String getZookeeperHosts() {
        return _zookeeperHosts;
    }

    public int getWriterTopicReplicationFactor() {
        return _writerTopicReplicationFactor;
    }
}