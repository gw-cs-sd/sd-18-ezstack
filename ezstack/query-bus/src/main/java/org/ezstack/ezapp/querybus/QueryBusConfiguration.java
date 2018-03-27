package org.ezstack.ezapp.querybus;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class QueryBusConfiguration {

    @Valid
    @NotEmpty
    @JsonProperty("bootstrapServers")
    private String _bootstrapServers;

    @Valid
    @NotEmpty
    @JsonProperty("producerName")
    private String _producerName;

    @Valid
    @NotEmpty
    @JsonProperty("queryBusTopicName")
    private String _queryTopicName;

    @Valid
    @NotNull
    @JsonProperty("queryBusTopicPartitionCount")
    private Integer _queryBusTopicPartitionCount;

    @Valid
    @NotEmpty
    @JsonProperty("zookeeperHosts")
    private String _zookeeperHosts;

    @Valid
    @NotNull
    @JsonProperty("queryBusTopicReplicationFactor")
    private Integer _queryBusTopicReplicationFactor;

    public String getBootstrapServers() {
        return _bootstrapServers;
    }

    public String getProducerName() {
        return _producerName;
    }

    public String getQueryBusTopicName() {
        return _queryTopicName;
    }

    public int getQueryBusTopicPartitionCount() {
        return _queryBusTopicPartitionCount;
    }

    public String getZookeeperHosts() {
        return _zookeeperHosts;
    }

    public int getQueryBusTopicReplicationFactor() {
        return _queryBusTopicReplicationFactor;
    }
}