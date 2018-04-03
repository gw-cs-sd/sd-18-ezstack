package org.ezstack.denormalizer.core.curator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.AbstractService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.ezstack.ezapp.datastore.api.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkNotNull;

public class CuratorRuleAcknowledger extends AbstractService {

    private static final Logger LOG = LoggerFactory.getLogger(CuratorRuleAcknowledger.class);

    private final static int BASE_RETRY_SLEEP_TYPE_IN_MS = 1000;
    private final static int MAX_CURATOR_RETRIES = 3;

    private static final ObjectMapper _mapper = new ObjectMapper();


    private final String _rulesPath;
    private final String _partitionId;

    private final CuratorFramework _client;
    private final TreeCache _ruleCache;

    // <rule table name, rule itself>
    private volatile ConcurrentHashMap<String, Rule> _rules;

    public CuratorRuleAcknowledger(String zookeeperHosts, String rulesPath, String partitionId) {
        _rulesPath = checkNotNull(rulesPath, "rulesPath");
        _partitionId = checkNotNull(partitionId, "partitionId");

        _client = CuratorFrameworkFactory.newClient(zookeeperHosts,
                new ExponentialBackoffRetry(BASE_RETRY_SLEEP_TYPE_IN_MS, MAX_CURATOR_RETRIES));

        _ruleCache = new TreeCache(_client, _rulesPath);

        _rules = new ConcurrentHashMap<>();

    }

    @Override
    protected void doStart() {
        try {
            _ruleCache.getListenable().addListener(this::updateTableForEvent);
            _client.start();
            _ruleCache.start();
        } catch (Exception e) {
            notifyFailed(e);
            throw new RuntimeException(e);
        }

        notifyStarted();
    }

    @Override
    protected void doStop() {
        try {
            _client.close();
            _ruleCache.close();
        } catch (Exception e) {
            notifyFailed(e);
            throw e;
        }

        notifyStopped();
    }

    private void acknowledgeRuleIfPending(Rule rule) {
        if (!rule.getStatus().equals(Rule.RuleStatus.PENDING)) {
            return;
        }

        try {
            _client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(ZKPaths.makePath(_rulesPath, rule.getTable(), "denormalizer", _partitionId));

        } catch (Exception e) {
            if (e instanceof KeeperException.NodeExistsException) {
                LOG.error("Already acknowled rule with table {} for partition id {}", rule.getTable(), _partitionId);
            }
            LOG.error(e.toString());
            throw new RuntimeException("Failed to acknowledge rule", e);
        }
    }

    private void handleAddOrUpdateEvent(TreeCacheEvent event) {
        ChildData childData = event.getData();
        int pathLength = ZKPaths.split(childData.getPath()).size();

        if (pathLength <= 1 || pathLength >= 3) {
            // This is the parent or another partition's acknowledgment, it can be ignored
            return;
        }

        try {
            Rule rule = _mapper.readValue(childData.getData(), Rule.class);
            _rules.put(rule.getTable(), rule);
            acknowledgeRuleIfPending(rule);
            LOG.info("Received update for rule with table {}. Current status is {}", rule.getTable(), rule.getStatus());
        } catch (IOException e) {
            LOG.error(e.toString());
            LOG.error("Rule cache is corrupted. Corrupt rules will be ignored");
        }
    }

    private void updateTableForEvent(CuratorFramework client, TreeCacheEvent event) {
        switch (event.getType()) {
            //fall through
            case NODE_ADDED:
            case NODE_UPDATED:
                handleAddOrUpdateEvent(event);
                return;
            case INITIALIZED:
                return;
            default:
                LOG.error("Zookeeper cache is experiencing issues");
                LOG.error(event.toString());
        }
    }

    public Set<Rule> getRules() {
        return ImmutableSet.copyOf(_rules.values());
    }
}
