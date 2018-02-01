package org.ezstack.ezapp.querybus;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.NotNull;

public class QueryBusConfiguration {

    @NotEmpty
    @JsonProperty("bootstrapServers")
    private String _bootstrapServers;

    @NotEmpty
    @JsonProperty("producerName")
    private String _producerName;

    @NotEmpty
    @JsonProperty("queryBusTopicName")
    private String _queryTopicName;

    @NotNull
    @JsonProperty("queryBusTopicPartitionCount")
    private Integer _queryBusTopicPartitionCount;

    @NotEmpty
    @JsonProperty("zookeeperHosts")
    private String _zookeeperHosts;

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
}