package org.ezstack.ezapp.querybus.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractService;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import kafka.utils.ZooKeeperClientWrapper;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.ezstack.ezapp.querybus.api.QueryMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.*;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class KafkaQueryBusPublisherDAO extends AbstractService {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaQueryBusPublisherDAO.class);

    private static final int MAX_PUBLISH_RETRIES = 3;
    private static final int BATCH_TIME_INTERVAL_MS = 250;
    private static final int REQUEST_TIMEOUT_MS_CONFIG = 3000;
    private static final int TRANSACTION_TIMEOUT_CONFIG = 3000;

    private static final int ZK_SESSION_TIMEOUT_IN_MS = 15 * 1000;
    private static final int ZK_CONNECTION_TIMEOUT_IN_MS = 10 * 1000;

    private Producer<String, JsonNode> _producer;

    private final String _bootstrapServers;
    private final String _producerName;
    private final String _queryBusTopic;
    private final String _zookeeperHosts;
    private final int _queryBusTopicPartitionCount;
    private final int _queryBusTopicReplicationFactor;

    private final ObjectMapper _objectMapper;

    @Inject
    public KafkaQueryBusPublisherDAO(@Named("bootstrapServers") String bootstrapServers,
                                     @Named("producerName") String producerName,
                                     @Named("queryBusTopic") String queryBusTopic,
                                     @Named("zookeeperHosts") String zookeeperHosts,
                                     @Named("queryBusTopicPartitionCount") int queryBusTopicPartitionCount,
                                     @Named("queryBusTopicReplicationFactor") int queryBusTopicReplicationFactor) {

        _bootstrapServers = checkNotNull(bootstrapServers, "bootstrapServers");
        _producerName = checkNotNull(producerName, "producerName");
        _queryBusTopic = checkNotNull(queryBusTopic, "queryBusTopic");
        _zookeeperHosts = checkNotNull(zookeeperHosts, "zookeeperHosts");

        checkArgument(queryBusTopicPartitionCount > 0, "Query Bus Partition Count must be > 0");
        checkArgument(queryBusTopicReplicationFactor > 0, "Query Bus Replication Factor must be > 0");

        _queryBusTopicPartitionCount = queryBusTopicPartitionCount;
        _queryBusTopicReplicationFactor = queryBusTopicReplicationFactor;

        _objectMapper = new ObjectMapper();

    }

    @Override
    protected void doStart() {
        createQueryBusTopic();

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, _bootstrapServers);
        props.put(ProducerConfig.RETRIES_CONFIG, MAX_PUBLISH_RETRIES);
        props.put(ProducerConfig.LINGER_MS_CONFIG, BATCH_TIME_INTERVAL_MS);

        props.put(ProducerConfig.CLIENT_ID_CONFIG, _producerName);

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, REQUEST_TIMEOUT_MS_CONFIG);
        props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, TRANSACTION_TIMEOUT_CONFIG);

        try {
            _producer = new KafkaProducer<String, JsonNode>(props);
        } catch (Exception e) {
            notifyFailed(e);
            throw e;
        }

        notifyStarted();
    }

    @Override
    protected void doStop() {
        try {
            _producer.close();
        } catch (Exception e) {
            notifyFailed(e);
            throw e;
        }
        notifyStopped();
    }

    private void createQueryBusTopic() {

        LOG.info("Creating topic {}", _queryBusTopic);

        ZooKeeperClientWrapper zkClientWrapper = null;
        try {
            zkClientWrapper = new ZooKeeperClientWrapper(new ZkClient(_zookeeperHosts, ZK_SESSION_TIMEOUT_IN_MS, ZK_CONNECTION_TIMEOUT_IN_MS, ZKStringSerializer$.MODULE$));
            ZkUtils zkUtils = new ZkUtils(zkClientWrapper, new ZkConnection(_zookeeperHosts), false);

            Properties topicConfiguration = new Properties();
            AdminUtils.createTopic(zkUtils, _queryBusTopic, _queryBusTopicPartitionCount, _queryBusTopicReplicationFactor,
                    topicConfiguration, RackAwareMode.Safe$.MODULE$);
        } catch (Exception e) {
            if (Throwables.getRootCause(e) instanceof TopicExistsException) {
                LOG.info("Topic {} already exists, proceeding without creation.", _queryBusTopic);
            } else {
                notifyFailed(e);
                throw e;
            }
        } finally {
            if (zkClientWrapper != null) {
                try {
                    zkClientWrapper.close();
                } catch (Exception e) {
                    notifyFailed(e);
                    throw e;
                }
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
