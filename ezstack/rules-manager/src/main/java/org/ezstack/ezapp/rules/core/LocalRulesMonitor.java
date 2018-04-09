package org.ezstack.ezapp.rules.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.name.Named;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import kafka.utils.ZooKeeperClientWrapper;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.zookeeper.KeeperException;
import org.ezstack.ezapp.datastore.api.Rule;
import org.ezstack.ezapp.datastore.api.RulesManager;
import org.ezstack.ezapp.datastore.api.ShutdownMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class LocalRulesMonitor extends AbstractService {

    private final static Logger LOG = LoggerFactory.getLogger(LocalRulesMonitor.class);

    private static final int MAX_BATCH_SIZE = 0;
    private static final int MAX_PUBLISH_RETRIES = 2;
    private static final int REQUEST_TIMEOUT_MS_CONFIG = 3000;
    private static final int TRANSACTION_TIMEOUT_CONFIG = 3000;
    private static final String ACKS_CONFIG = "all";

    private static final int ZK_SESSION_TIMEOUT_IN_MS = 15 * 1000;
    private static final int ZK_CONNECTION_TIMEOUT_IN_MS = 10 * 1000;

    private final static ObjectMapper _mapper = new ObjectMapper();

    private final RulesManager _rulesManager;
    private final CuratorFactory _curatorFactory;

    private final String _kafkaBootstrapServers;

    private CuratorFramework _curator;

    private ScheduledExecutorService _service;

    public LocalRulesMonitor(RulesManager rulesManager, CuratorFactory curatorFactory, String bootstrapServers) {
        _rulesManager = rulesManager;
        _curatorFactory = curatorFactory;
        _kafkaBootstrapServers = bootstrapServers;
    }

    @Override
    protected void doStart() {
        try {
            _curator = _curatorFactory.getStartedCuratorFramework();
            createKafkaTopic("bootstrapped-document-messages", 6, 1);
            createKafkaTopic("shutdown-message", 1, 1);
            writeShutdownMessageToTopic("shutdown-message");
        } catch (Exception e) {
            notifyFailed(e);
            throw e;
        }

        _service = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("rules-monitor-%d").build());

        _service.scheduleAtFixedRate(this::processRules, 30, 20, TimeUnit.SECONDS);

        notifyStarted();
    }

    private void createKafkaTopic(String topicName, int numPartitions, int replicationFactor) {
        ZooKeeperClientWrapper zkClientWrapper = null;
        String zkHosts = _curatorFactory.getZkHosts();
        try {
            zkClientWrapper = new ZooKeeperClientWrapper(new ZkClient(zkHosts, ZK_SESSION_TIMEOUT_IN_MS, ZK_CONNECTION_TIMEOUT_IN_MS, ZKStringSerializer$.MODULE$));
            ZkUtils zkUtils = new ZkUtils(zkClientWrapper, new ZkConnection(zkHosts), false);

            Properties topicConfiguration = new Properties();
            AdminUtils.createTopic(zkUtils, topicName, numPartitions,
                    replicationFactor, topicConfiguration, RackAwareMode.Safe$.MODULE$);
        } catch (Exception e) {

            if (Throwables.getRootCause(e) instanceof TopicExistsException) {
                LOG.info("Topic {} already exists, proceeding without creation.", topicName);
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
                }
            }
        }
    }

    private void writeShutdownMessageToTopic(String topicName) {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, _kafkaBootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, ACKS_CONFIG);
        props.put(ProducerConfig.RETRIES_CONFIG, MAX_PUBLISH_RETRIES);

        props.put(ProducerConfig.BATCH_SIZE_CONFIG, MAX_BATCH_SIZE);

        props.put(ProducerConfig.CLIENT_ID_CONFIG, this.getClass().getSimpleName());

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, REQUEST_TIMEOUT_MS_CONFIG);
        props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, TRANSACTION_TIMEOUT_CONFIG);

        KafkaProducer producer = new KafkaProducer<String, JsonNode>(props);

        Futures.getUnchecked(producer.send(new ProducerRecord(topicName, _mapper.valueToTree(ShutdownMessage.instance()))));

        producer.close();

    }

    private void processRules() {
        bootstrapAcceptedRules();
        acceptAcknowledgedRules();
    }

    private void acceptAcknowledgedRules() {
        _rulesManager.getRules(Rule.RuleStatus.PENDING).parallelStream()
                .forEach(rule -> {
                    try {
                        // TODO: replace six with an injected partition count
                        // TODO: replace rules path with an injected string
                        if (_curator.getChildren().
                                forPath(ZKPaths.makePath("/rules", rule.getTable(), "denormalizer"))
                                .size() == 6) {
                            _rulesManager.setRuleStatus(rule.getTable(), Rule.RuleStatus.ACCEPTED);
                        }
                    } catch (KeeperException.NoNodeException e) {
                        LOG.info("Still waiting for rule {} to be acknowledged");
                    } catch (Exception e) {
                        notifyFailed(e);
                    }
                });
    }

    private void bootstrapAcceptedRules() {
        Set<Rule> acceptedRules = _rulesManager.getRules(Rule.RuleStatus.ACCEPTED);

        if (acceptedRules.isEmpty()) {
            return;
        }

        try {
            _curator.create()
                    .creatingParentsIfNeeded()
                    .forPath(ZKPaths.makePath("/bootstrapper", "INSERT_JOB_ID_HERE"), _mapper.writeValueAsBytes(acceptedRules));
        } catch (Exception e) {
            notifyFailed(e);
            throw new RuntimeException(e);
        }

        // TODO: schedule job with the job id used above

        acceptedRules.parallelStream()
                .forEach(rule -> _rulesManager.setRuleStatus(rule.getTable(), Rule.RuleStatus.BOOTSTRAPPING));
    }

    @Override
    protected void doStop() {
        try {
            _curator.close();
        } catch (Exception e) {
            notifyFailed(e);
            throw e;
        }

        notifyStopped();
    }


}
