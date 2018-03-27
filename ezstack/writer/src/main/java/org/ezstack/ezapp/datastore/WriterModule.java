package org.ezstack.ezapp.datastore;

import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import org.ezstack.ezapp.common.lifecycle.GuavaManagedService;
import org.ezstack.ezapp.common.lifecycle.LifeCycleRegistry;
import org.ezstack.ezapp.datastore.api.DataWriter;
import org.ezstack.ezapp.datastore.core.DefaultDataWriter;
import org.ezstack.ezapp.datastore.db.kafka.KafkaDataWriterDAO;

public class WriterModule extends PrivateModule {

    private final WriterConfiguration _configuration;

    public WriterModule(WriterConfiguration writerConfiguration) {
        _configuration = writerConfiguration;
    }

    protected void configure() {
        bind(DataWriter.class).to(DefaultDataWriter.class).asEagerSingleton();
        expose(DataWriter.class);
    }

    @Provides
    @Singleton
    KafkaDataWriterDAO provideKafkaDataWriterDAO(@Named("bootstrapServers") String bootstrapServers,
                                                 @Named("producerName") String producerName,
                                                 @Named("documentTopicPartitionCount") int documentTopicPartitionCount,
                                                 @Named("documentTopic") String documentTopic,
                                                 @Named("zookeeperHosts") String zookeeperHosts,
                                                 @Named("documentTopicReplicationFactor") int documentTopicReplicationFactor,
                                                 LifeCycleRegistry lifeCycleRegistry) {
        KafkaDataWriterDAO kafkaDataWriterDAO = new KafkaDataWriterDAO(bootstrapServers, producerName, documentTopic,
                zookeeperHosts, documentTopicPartitionCount, documentTopicReplicationFactor);
        lifeCycleRegistry.manage(new GuavaManagedService(kafkaDataWriterDAO));
        return kafkaDataWriterDAO;
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
    @Named("documentTopicPartitionCount")
    int provideDocumentTopicPartitionCount() {
        return _configuration.getWriterTopicPartitionCount();
    }

    @Provides
    @Singleton
    @Named("documentTopic")
    String provideDocumentTopic() {
        return _configuration.getWriterTopicName();
    }

    @Provides
    @Singleton
    @Named("zookeeperHosts")
    String provideZookeeperHosts() {
        return _configuration.getZookeeperHosts();
    }

    @Provides
    @Singleton
    @Named("documentTopicReplicationFactor")
    int provideDocumentTopicReplicationFactor() {
        return _configuration.getWriterTopicReplicationFactor();
    }
}
