package org.ezstack.ezapp.querybus;

import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import org.ezstack.ezapp.common.lifecycle.GuavaManagedService;
import org.ezstack.ezapp.common.lifecycle.LifeCycleRegistry;
import org.ezstack.ezapp.querybus.api.QueryBusPublisher;
import org.ezstack.ezapp.querybus.core.DefaultQueryBusPublisher;
import org.ezstack.ezapp.querybus.core.KafkaQueryBusPublisherDAO;

import java.util.Properties;

public class QueryBusModule extends PrivateModule {

    private final QueryBusConfiguration _configuration;

    public QueryBusModule(QueryBusConfiguration configuration) {
        _configuration = configuration;
    }

    @Override
    protected void configure() {
        bind(QueryBusPublisher.class).to(DefaultQueryBusPublisher.class).asEagerSingleton();
        expose(QueryBusPublisher.class);
    }

    @Provides
    @Singleton
    KafkaQueryBusPublisherDAO provideKafkaQueryBusPublisherDAO(@Named("bootstrapServers") String bootstrapServers,
                                                               @Named("producerName") String producerName,
                                                               @Named("queryBusTopicPartitionCount") int queryBusPartitionCount,
                                                               @Named("queryBusTopic") String queryBusTopic,
                                                               @Named("zookeeperHosts") String zookeeperHosts,
                                                               @Named("queryBusTopicReplicationFactor") int queryBusTopicReplicationFactor,
                                                               LifeCycleRegistry lifeCycleRegistry) {
        KafkaQueryBusPublisherDAO kafkaQueryBusPublisherDAO = new KafkaQueryBusPublisherDAO(bootstrapServers,
                producerName,  queryBusTopic, zookeeperHosts, queryBusPartitionCount, queryBusTopicReplicationFactor);
        lifeCycleRegistry.manage(new GuavaManagedService(kafkaQueryBusPublisherDAO));
        return kafkaQueryBusPublisherDAO;
    }

    @Provides
    @Singleton
    @Named("bootstrapServers")
    String provideBootstrapServer() {
        return _configuration.getBootstrapServers();
    }

    @Provides
    @Singleton
    @Named("producerName")
    String provideProducerName() {
        return _configuration.getProducerName();
    }

    @Provides
    @Singleton
    @Named("queryBusTopicPartitionCount")
    int provideQueryBusTopicPartitionCount() {
        return _configuration.getQueryBusTopicPartitionCount();
    }

    @Provides
    @Singleton
    @Named("queryBusTopic")
    String provideQueryBusTopic() {
        return _configuration.getQueryBusTopicName();
    }

    @Provides
    @Singleton
    @Named("zookeeperHosts")
    String provideZookeeperHosts() {
        return _configuration.getZookeeperHosts();
    }

    @Provides
    @Singleton
    @Named("queryBusTopicReplicationFactor")
    int provideQueryBusTopicReplicationFactor() {
        return _configuration.getQueryBusTopicReplicationFactor();
    }
}
