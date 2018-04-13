package org.ezstack.ezapp.rules.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.AbstractService;
import com.google.inject.Inject;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.ezstack.ezapp.datastore.api.Rule;
import org.ezstack.ezapp.datastore.api.RuleAlreadyExistsException;
import org.ezstack.ezapp.datastore.api.RulesManager;
import org.ezstack.ezapp.rules.api.RulesPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class CuratorRulesManager extends AbstractService implements RulesManager {

    private CuratorFramework _client;

    private final CuratorFactory _curatorFactory;

    private final String _rulesPath;
    private final ObjectMapper _mapper;

    private final static Logger LOG = LoggerFactory.getLogger(CuratorRulesManager.class);

    @Inject
    public CuratorRulesManager(@RulesPath String rulesPath,
                               CuratorFactory curatorFactory) {

        _rulesPath = checkNotNull(rulesPath, "rulesPath");
        _curatorFactory = checkNotNull(curatorFactory, "curatorFactory");
        _mapper = new ObjectMapper();
    }

    @Override
    protected void doStart() {
        try {
            _client = _curatorFactory.getStartedCuratorFramework();

            _client.start();

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
        } catch (Exception e) {
            notifyFailed(e);
            throw e;
        }

        notifyStopped();
    }

    @Override
    public void createRule(Rule rule) throws RuleAlreadyExistsException {
        // check to make sure they are submitting a pending rule
        checkArgument(rule.getStatus() == Rule.RuleStatus.PENDING);
        try {
            _client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(ZKPaths.makePath(_rulesPath, rule.getTable()), _mapper.writeValueAsBytes(rule));

        } catch (Exception e) {
            if (e instanceof KeeperException.NodeExistsException) {
                throw new RuleAlreadyExistsException(rule.getTable());
            }
            LOG.error(e.toString());
            throw new RuntimeException("Failed to create rule", e);
        }
    }

    @Override
    public void removeRule(String ruleTable) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setRuleStatus(String ruleTable, Rule.RuleStatus status) {
        Rule rule = getRule(ruleTable);
        if (rule == null) {
            throw new IllegalArgumentException("Rule does not exist");
        }

        try {
            _client.setData()
                    .forPath(ZKPaths.makePath(_rulesPath, ruleTable),
                            _mapper.writeValueAsBytes(new Rule(rule.getQuery(), ruleTable, status)));

        } catch (Exception e) {
            throw new RuntimeException("Failed to set rule status", e);
        }
    }

    @Override
    public Rule getRule(String ruleTable) {
        try {
            return _mapper.readValue(_client.getData().forPath(ZKPaths.makePath(_rulesPath, ruleTable)), Rule.class);
        } catch (Exception e) {
            if (e instanceof KeeperException.NoNodeException) {
                return null;
            }
            throw new RuntimeException("Failed to retrieve rule", e);
        }
    }

    @Override
    public Set<Rule> getRules() {
        try {
           return  _client.getChildren().forPath(_rulesPath)
                    .parallelStream()
                    .map(child -> {
                        try {
                            return _mapper.readValue(_client.getData().forPath(ZKPaths.makePath(_rulesPath, child)), Rule.class);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .collect(Collectors.toSet());
        } catch (KeeperException.NoNodeException e) {
            return Collections.emptySet();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Set<Rule> getRules(Rule.RuleStatus status) {
        return getRules().stream()
                .filter(rule -> rule.getStatus() == status)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<Rule> getRules(String outerTable, Rule.RuleStatus status) {
        return getRules().stream()
                .filter(rule -> rule.getQuery().getTable().equals(outerTable) && rule.getStatus() == status)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<Rule> getRules(String outerTable, String innerTable, Rule.RuleStatus status) {
        return getRules().stream()
                .filter(rule -> rule.getStatus() == status)
                .filter(rule -> rule.getQuery().getJoin() != null)
                .filter(rule -> rule.getQuery().getTable().equals(outerTable))
                .filter(rule -> rule.getQuery().getJoin().getTable().equals(innerTable))
                .collect(Collectors.toSet());
    }
}
