package org.ezstack.ezapp.rules.core;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import kafka.utils.ZooKeeperClientWrapper;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorOp;
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
import org.ezstack.ezapp.jobmanager.api.JobManager;
import org.ezstack.ezapp.rules.config.BootstrapperConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class LocalRulesMonitor extends AbstractService {

    private final static Logger LOG = LoggerFactory.getLogger(LocalRulesMonitor.class);

    private final static int INITIAL_DELAY_SECONDS = 30;
    private final static int SERVICE_INTERVAL_SECONDS = 30;

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
    private final int _partitionCount;
    private final int _replicationFactor;
    private final String _bootstrapTopicName;
    private final String _shutdownTopicName;
    private final String _rulesPath;
    private final String _bootstrapperPath;
    private final BootstrapperConfig _bootstrapperConfig;
    private final JobManager _jobManager;

    private CuratorFramework _curator;

    private ScheduledExecutorService _service;

    LocalRulesMonitor(RulesManager rulesManager, CuratorFactory curatorFactory, String bootstrapServers,
                      int partitionCount, int replicationFactor, String bootstrapTopicName, String shutdownTopicName,
                      String rulesPath, String bootstrapperPath, BootstrapperConfig bootstrapperConfig,
                      JobManager jobManager) {
        _rulesManager = checkNotNull(rulesManager, "rulesManager");
        _curatorFactory = checkNotNull(curatorFactory, "curatorFactory");
        _kafkaBootstrapServers = checkNotNull(bootstrapServers, "bootstrapServers");
        _bootstrapTopicName = checkNotNull(bootstrapTopicName, "bootstrapTopicName");
        _shutdownTopicName = checkNotNull(shutdownTopicName, "shutdownTopicName");
        _rulesPath = checkNotNull(rulesPath, "rulesPath");
        _bootstrapperPath = checkNotNull(bootstrapperPath, "bootstrapperPath");
        _bootstrapperConfig = checkNotNull(bootstrapperConfig, "bootstrapperConfig");
        _jobManager = checkNotNull(jobManager, "jobManager");

        checkArgument(partitionCount > 0);
        checkArgument(replicationFactor > 0);


        _partitionCount = partitionCount;
        _replicationFactor = replicationFactor;
    }

    @Override
    protected void doStart() {
        try {
            _curator = _curatorFactory.getStartedCuratorFramework();
            _curator.createContainers(_bootstrapperPath);
            createKafkaTopicIfNotExists(_bootstrapTopicName, _partitionCount, _replicationFactor);
            createKafkaTopicIfNotExists(_shutdownTopicName, 1, _replicationFactor);
            writeShutdownMessageToTopic(_shutdownTopicName);
        } catch (Exception e) {
            notifyFailed(e);
            throw new RuntimeException(e);
        }

        _service = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("rules-monitor-%d").build());

        _service.scheduleAtFixedRate(this::processRules, INITIAL_DELAY_SECONDS, SERVICE_INTERVAL_SECONDS, TimeUnit.SECONDS);

        notifyStarted();
    }

    private void createKafkaTopicIfNotExists(String topicName, int numPartitions, int replicationFactor) {
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
        try {
            acceptAcknowledgedRules();
            scheduleAcceptedRules();
            activateBootstrappedRules();

        } catch (Exception e) {
            notifyFailed(e);
            throw new RuntimeException(e);
        }
    }

    private void acceptAcknowledgedRules() throws Exception {
        _rulesManager.getRules(Rule.RuleStatus.PENDING).parallelStream()
                .forEach(rule -> {
                    try {
                        if (_curator.getChildren().
                                forPath(ZKPaths.makePath(_rulesPath, rule.getTable(), "denormalizer"))
                                .size() == _partitionCount) {
                            _rulesManager.setRuleStatus(rule.getTable(), Rule.RuleStatus.ACCEPTED);
                        }
                    } catch (KeeperException.NoNodeException e) {
                        LOG.info("Still waiting for rule {} to be acknowledged", rule.toString());
                    } catch (Exception e) {
                        notifyFailed(e);
                        throw new RuntimeException(e);
                    }
                });
    }

    private void scheduleAcceptedRules() throws Exception {
        Set<Rule> acceptedRules = _rulesManager.getRules(Rule.RuleStatus.ACCEPTED);

        if (acceptedRules.isEmpty()) {
            return;
        }

        List<CuratorOp> ops = getCuratorOpsForStatusSetters(acceptedRules, Rule.RuleStatus.BOOTSTRAPPING);
        String bootstrapperJobId = UUID.randomUUID().toString();
        ops.add(_curator.transactionOp().create()
                .forPath(ZKPaths.makePath(_bootstrapperPath, bootstrapperJobId), _mapper.writeValueAsBytes(acceptedRules)));

        for (Rule rule : acceptedRules) {
            ops.add(_curator.transactionOp().create()
                .forPath(ZKPaths.makePath(_rulesPath, rule.getTable(), _bootstrapperPath), bootstrapperJobId.getBytes(Charsets.UTF_8)));
        }

        _curator.transaction().forOperations(ops);

        // TODO: start boostrapper job here
        _jobManager.create(_bootstrapperConfig.forJobId(bootstrapperJobId));
        LOG.info("Starting job with id: {}", bootstrapperJobId);
    }

    private List<CuratorOp> getCuratorOpsForStatusSetters(Set<Rule> rules, Rule.RuleStatus status) throws Exception {
        List<CuratorOp> ops = new LinkedList<>();
        for (Rule rule : rules) {
            ops.add(_curator.transactionOp().setData()
                    .forPath(ZKPaths.makePath(_rulesPath, rule.getTable()),
                            _mapper.writeValueAsBytes(new Rule(rule.getQuery(), rule.getTable(), status))));

        }

        return ops;
    }

    private void activateBootstrappedRules() throws Exception {
        for (String child : _curator.getChildren().forPath(_bootstrapperPath)) {
            if (_curator.getChildren().forPath(ZKPaths.makePath(_bootstrapperPath, child)).size() == _partitionCount) {
                Set<Rule> rules = _mapper.readValue(_curator.getData().forPath(ZKPaths.makePath(_bootstrapperPath, child)),
                        new TypeReference<Set<Rule>>() {});
                if (rules.isEmpty()) {
                    _curator.delete().deletingChildrenIfNeeded().forPath(ZKPaths.makePath(_bootstrapperPath, child));
                    break;
                }
                List<CuratorOp> ops = getCuratorOpsForStatusSetters(rules, Rule.RuleStatus.ACTIVE);
                ops.add(_curator.transactionOp().setData().forPath(ZKPaths.makePath(_bootstrapperPath, child), "[]".getBytes(Charsets.UTF_8)));
                _curator.transaction().forOperations(ops);
            }
        }
    }

    @Override
    protected void doStop() {
        try {
            _curator.close();
            _service.shutdown();
            if (!_service.awaitTermination(10, TimeUnit.SECONDS)) {
                LOG.error("{} is this running after 10 seconds", this.getClass().getSimpleName());
            }
        } catch (Exception e) {
            notifyFailed(e);
            throw new RuntimeException(e);
        }

        notifyStopped();
    }


}
