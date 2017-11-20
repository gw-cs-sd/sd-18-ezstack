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

    protected void configure() {
        bind(DataWriter.class).to(DefaultDataWriter.class).asEagerSingleton();
        bind(KafkaDataWriterDAO.class).asEagerSingleton();
        expose(DataWriter.class);
    }

    @Provides
    @Singleton
    Producer<String, JsonNode> provideProducer(WriterConfiguration configuration) {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, configuration.getBootstrapServers());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);

        // batch size will remain at 0 until it is proven that this doesn't endanger durability
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 0);

        props.put(ProducerConfig.CLIENT_ID_CONFIG, configuration.getProducerName());

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000);
        props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 1000);
        // TODO: find a way to call producer.close when service terminates
        return new KafkaProducer<String, JsonNode>(props);
    }

    @Provides
    @Singleton
    @Named("documentTopic")
    String provideDocumentTopic(WriterConfiguration configuration) {
        return configuration.getWriterTopicName();
    }

    @Provides
    @Singleton
    @Named("zookeeperHosts")
    String provideZookeeperHosts(WriterConfiguration configuration) {
        return configuration.getZookeeperHosts();
    }
}
