package org.ezstack.ezapp.querybus.core;

import com.google.inject.Inject;
import org.ezstack.ezapp.datastore.api.Query;
import org.ezstack.ezapp.querybus.api.QueryBusPublisher;
import org.ezstack.ezapp.querybus.api.QueryMetadata;

public class DefaultQueryBusPublisher implements QueryBusPublisher {

    private final KafkaQueryBusPublisherDAO _kafkaQueryBusPublisherDAO;

    @Inject
    public DefaultQueryBusPublisher(KafkaQueryBusPublisherDAO kafkaQueryBusPublisherDAO) {
        _kafkaQueryBusPublisherDAO = kafkaQueryBusPublisherDAO;
    }

    @Override
    public void publishQueryAsync(Query query, long responseTimeMs) {
        _kafkaQueryBusPublisherDAO.publishQueryMetadataAsync(new QueryMetadata(query, responseTimeMs));
    }
}
