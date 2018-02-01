package org.ezstack.ezapp.datastore;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import org.apache.kafka.connect.json.JsonSerializer;
import org.ezstack.ezapp.datastore.api.DataWriter;
import org.ezstack.ezapp.datastore.core.DefaultDataWriter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.ezstack.ezapp.datastore.db.kafka.KafkaDataWriterDAO;

import java.util.Properties;

public class WriterModule extends PrivateModule {
    // batch size will remain at 0 until it is proven that this doesn't endanger durability
    private static final int MAX_BATCH_SIZE = 0;
    private static final int MAX_PUBLISH_RETRIES = 2;
    private static final int REQUEST_TIMEOUT_MS_CONFIG = 3000;
    private static final int TRANSACTION_TIMEOUT_CONFIG = 3000;
    private static final String ACKS_CONFIG = "all";


    private final WriterConfiguration _configuration;

    public WriterModule(WriterConfiguration writerConfiguration) {
        _configuration = writerConfiguration;
    }

    protected void configure() {
        bind(DataWriter.class).to(DefaultDataWriter.class).asEagerSingleton();
        bind(KafkaDataWriterDAO.class).asEagerSingleton();
        expose(DataWriter.class);
    }

    @Provides
    @Singleton
    Producer<String, JsonNode> provideProducer() {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, _configuration.getBootstrapServers());
        props.put(ProducerConfig.ACKS_CONFIG, ACKS_CONFIG);
        props.put(ProducerConfig.RETRIES_CONFIG, MAX_PUBLISH_RETRIES);

        props.put(ProducerConfig.BATCH_SIZE_CONFIG, MAX_BATCH_SIZE);

        props.put(ProducerConfig.CLIENT_ID_CONFIG, _configuration.getProducerName());

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, REQUEST_TIMEOUT_MS_CONFIG);
        props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, TRANSACTION_TIMEOUT_CONFIG);
        // TODO: find a way to call producer.close when service terminates
        return new KafkaProducer<String, JsonNode>(props);
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
}
