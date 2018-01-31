package org.ezstack.ezapp.querybus;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonSerializer;
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
        bind(KafkaQueryBusPublisherDAO.class).asEagerSingleton();
        bind(QueryBusPublisher.class).to(DefaultQueryBusPublisher.class).asEagerSingleton();
        expose(QueryBusPublisher.class);
    }

    @Provides
    @Singleton
    Producer<String, JsonNode> provideProducer() {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, _configuration.getBootstrapServers());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 250);

        props.put(ProducerConfig.CLIENT_ID_CONFIG, _configuration.getProducerName());

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 3000);
        props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 3000);
        // TODO: find a way to call producer.close when service terminates
        return new KafkaProducer<String, JsonNode>(props);
    }

    @Provides
    @Singleton
    @Named("queryBusTopicPartitionCount")
    int provideDocumentTopicPartitionCount() {
        return _configuration.getQueryBusTopicPartitionCount();
    }

    @Provides
    @Singleton
    @Named("queryBusTopic")
    String provideDocumentTopic() {
        return _configuration.getQueryBusTopicName();
    }

    @Provides
    @Singleton
    @Named("zookeeperHosts")
    String provideZookeeperHosts() {
        return _configuration.getZookeeperHosts();
    }
}
