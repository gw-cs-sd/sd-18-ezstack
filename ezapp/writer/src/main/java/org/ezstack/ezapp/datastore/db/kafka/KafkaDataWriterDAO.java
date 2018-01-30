package org.ezstack.ezapp.datastore.db.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
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
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.ezstack.ezapp.datastore.api.KeyBuilder;
import org.ezstack.ezapp.datastore.api.Update;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static com.google.common.base.Preconditions.checkNotNull;

public class KafkaDataWriterDAO {

    private final Logger _log = LoggerFactory.getLogger(KafkaDataWriterDAO.class);

    private static final int ZK_SESSION_TIMEOUT_IN_MS = 15 * 1000;
    private static final int ZK_CONNECTION_TIMEOUT_IN_MS = 10 * 1000;

    private final Producer<String, JsonNode> _producer;
    private final String _documentTopic;
    private final String _zookeeperHosts;
    private final int _documentTopicPartitionCount;
    private final ObjectMapper _objectMapper;

    @Inject
    public KafkaDataWriterDAO(Producer<String, JsonNode> producer, @Named("documentTopic") String documentTopic,
                              @Named("zookeeperHosts") String zookeeperHosts,
                              @Named("documentTopicPartitionCount") int documentTopicPartitionCount) {
        checkNotNull(producer, "producer");
        checkNotNull(documentTopic, "documentTopic");
        checkNotNull(zookeeperHosts, "zookeeperHosts");

        _producer = producer;
        _documentTopic = documentTopic;
        _zookeeperHosts = zookeeperHosts;
        _documentTopicPartitionCount = documentTopicPartitionCount;
        _objectMapper = new ObjectMapper();

        createDocumentTopic();
    }

    private void createDocumentTopic() {

        _log.info("Creating topic {}", _documentTopic);

        ZooKeeperClientWrapper zkClientWrapper = null;
        try {
            zkClientWrapper = new ZooKeeperClientWrapper(new ZkClient(_zookeeperHosts, ZK_SESSION_TIMEOUT_IN_MS, ZK_CONNECTION_TIMEOUT_IN_MS, ZKStringSerializer$.MODULE$));
            ZkUtils zkUtils = new ZkUtils(zkClientWrapper, new ZkConnection(_zookeeperHosts), false);

            Properties topicConfiguration = new Properties();
            // TODO: make replication factor configurable
            AdminUtils.createTopic(zkUtils, _documentTopic, _documentTopicPartitionCount, 1, topicConfiguration, RackAwareMode.Safe$.MODULE$);
        } catch (Exception e) {
            if (Throwables.getRootCause(e) instanceof TopicExistsException) {
                _log.info("Topic {} already exists, proceeding without creation.", _documentTopic);
            } else {
                Throwables.throwIfUnchecked(e);
            }
        } finally {
            if (zkClientWrapper != null) {
                zkClientWrapper.close();
            }
        }
    }

    public void update(Update update) {
        Futures.getUnchecked(_producer.send(new ProducerRecord<String, JsonNode>(_documentTopic,
                KeyBuilder.hashKey(update.getTable(), update.getKey()),
                _objectMapper.valueToTree(update))));
    }
}
