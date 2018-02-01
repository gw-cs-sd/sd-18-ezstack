package org.ezstack.ezapp.querybus.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import kafka.utils.ZooKeeperClientWrapper;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.TopicExistsException;
import org.ezstack.ezapp.querybus.api.QueryMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.*;

import static com.google.common.base.Preconditions.checkNotNull;

public class KafkaQueryBusPublisherDAO {

    private final Logger _log = LoggerFactory.getLogger(KafkaQueryBusPublisherDAO.class);

    private static final int ZK_SESSION_TIMEOUT_IN_MS = 15 * 1000;
    private static final int ZK_CONNECTION_TIMEOUT_IN_MS = 10 * 1000;

    private final Producer<String, JsonNode> _producer;
    private final String _queryBusTopic;
    private final String _zookeeperHosts;
    private final int _queryBusTopicPartitionCount;
    private final ObjectMapper _objectMapper;

    @Inject
    public KafkaQueryBusPublisherDAO(Producer<String, JsonNode> producer, @Named("queryBusTopic") String queryBusTopic,
                              @Named("zookeeperHosts") String zookeeperHosts,
                              @Named("queryBusTopicPartitionCount") int queryBusTopicPartitionCount) {
        checkNotNull(producer, "producer");
        checkNotNull(queryBusTopic, "queryBusTopic");
        checkNotNull(zookeeperHosts, "zookeeperHosts");

        _producer = producer;
        _queryBusTopic = queryBusTopic;
        _zookeeperHosts = zookeeperHosts;
        _queryBusTopicPartitionCount = queryBusTopicPartitionCount;
        _objectMapper = new ObjectMapper();

        createQueryBusTopic();
    }


    private void createQueryBusTopic() {

        _log.info("Creating topic {}", _queryBusTopic);

        ZooKeeperClientWrapper zkClientWrapper = null;
        try {
            zkClientWrapper = new ZooKeeperClientWrapper(new ZkClient(_zookeeperHosts, ZK_SESSION_TIMEOUT_IN_MS, ZK_CONNECTION_TIMEOUT_IN_MS, ZKStringSerializer$.MODULE$));
            ZkUtils zkUtils = new ZkUtils(zkClientWrapper, new ZkConnection(_zookeeperHosts), false);

            Properties topicConfiguration = new Properties();
            // TODO: make replication factor configurable
            AdminUtils.createTopic(zkUtils, _queryBusTopic, _queryBusTopicPartitionCount, 1, topicConfiguration, RackAwareMode.Safe$.MODULE$);
        } catch (Exception e) {
            if (Throwables.getRootCause(e) instanceof TopicExistsException) {
                _log.info("Topic {} already exists, proceeding without creation.", _queryBusTopic);
            } else {
                Throwables.throwIfUnchecked(e);
            }
        } finally {
            if (zkClientWrapper != null) {
                zkClientWrapper.close();
            }
        }
    }

    public void publishQueryMetadataAsync(QueryMetadata queryMetadata) {
        Future<RecordMetadata> future = _producer.send(new ProducerRecord<String, JsonNode>(_queryBusTopic, queryMetadata.getQueryIdentifier(),
                _objectMapper.valueToTree(queryMetadata)), new Callback() {
            // TODO: add a metric for failures and successes
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    throw new RuntimeException(exception);
                }
            }
        });

    }
}
