package org.ezstack.ezapp.datastore.db.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.Futures;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import kafka.utils.ZooKeeperClientWrapper;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.ezstack.ezapp.datastore.api.KeyBuilder;
import org.ezstack.ezapp.datastore.api.Update;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class KafkaDataWriterDAO extends AbstractService {

    private final static Logger LOG = LoggerFactory.getLogger(KafkaDataWriterDAO.class);

    // batch size will remain at 0 until it is proven that this doesn't endanger durability
    private static final int MAX_BATCH_SIZE = 0;
    private static final int MAX_PUBLISH_RETRIES = 2;
    private static final int REQUEST_TIMEOUT_MS_CONFIG = 3000;
    private static final int TRANSACTION_TIMEOUT_CONFIG = 3000;
    private static final String ACKS_CONFIG = "all";

    private static final int ZK_SESSION_TIMEOUT_IN_MS = 15 * 1000;
    private static final int ZK_CONNECTION_TIMEOUT_IN_MS = 10 * 1000;

    private Producer<String, JsonNode> _producer;

    private final String _bootstrapServers;
    private final String _producerName;
    private final String _documentTopic;
    private final String _zookeeperHosts;
    private final int _documentTopicPartitionCount;
    private final int _documentTopicReplicationFactor;
    private final ObjectMapper _objectMapper;

    @Inject
    public KafkaDataWriterDAO(@Named("bootstrapServers") String bootstrapServers,
                              @Named("producerName") String producerName,
                              @Named("documentTopic") String documentTopic,
                              @Named("zookeeperHosts") String zookeeperHosts,
                              @Named("documentTopicPartitionCount") int documentTopicPartitionCount,
                              @Named("documentTopicReplicationFactor") int documentTopicReplicationFactor) {
        _bootstrapServers = checkNotNull(bootstrapServers, "bootstrapServers");
        _producerName = checkNotNull(producerName, "producerName");
        _documentTopic = checkNotNull(documentTopic, "documentTopic");
        _zookeeperHosts = checkNotNull(zookeeperHosts, "zookeeperHosts");
        checkArgument(documentTopicPartitionCount > 0, "Document topic partition count must be > 0");
        checkArgument(documentTopicReplicationFactor > 0, "Document topic replication factor must be > 0");

        _documentTopicPartitionCount = documentTopicPartitionCount;
        _documentTopicReplicationFactor = documentTopicReplicationFactor;
        _objectMapper = new ObjectMapper();

    }

    @Override
    protected void doStart() {

        createDocumentTopic();

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, _bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, ACKS_CONFIG);
        props.put(ProducerConfig.RETRIES_CONFIG, MAX_PUBLISH_RETRIES);

        props.put(ProducerConfig.BATCH_SIZE_CONFIG, MAX_BATCH_SIZE);

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

    private void createDocumentTopic() {

        LOG.info("Creating topic {}", _documentTopic);

        ZooKeeperClientWrapper zkClientWrapper = null;
        try {
            zkClientWrapper = new ZooKeeperClientWrapper(new ZkClient(_zookeeperHosts, ZK_SESSION_TIMEOUT_IN_MS, ZK_CONNECTION_TIMEOUT_IN_MS, ZKStringSerializer$.MODULE$));
            ZkUtils zkUtils = new ZkUtils(zkClientWrapper, new ZkConnection(_zookeeperHosts), false);

            Properties topicConfiguration = new Properties();
            AdminUtils.createTopic(zkUtils, _documentTopic, _documentTopicPartitionCount,
                    _documentTopicReplicationFactor, topicConfiguration, RackAwareMode.Safe$.MODULE$);
        } catch (Exception e) {

            if (Throwables.getRootCause(e) instanceof TopicExistsException) {
                LOG.info("Topic {} already exists, proceeding without creation.", _documentTopic);
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

    public void update(Update update) {
        Futures.getUnchecked(_producer.send(new ProducerRecord<String, JsonNode>(_documentTopic,
                KeyBuilder.hashKey(update.getTable(), update.getKey()),
                _objectMapper.valueToTree(update))));
    }
}
