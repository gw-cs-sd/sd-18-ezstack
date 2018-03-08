package org.ezstack.ezapp.rules.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.*;
import com.google.inject.Inject;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.utils.ZKPaths;
import org.apache.logging.log4j.util.Strings;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.ezstack.ezapp.datastore.api.Rule;
import org.ezstack.ezapp.datastore.api.RuleAlreadyExistsException;
import org.ezstack.ezapp.datastore.api.RulesManager;
import org.ezstack.ezapp.rules.api.RulesPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.MoreObjects.firstNonNull;
import static org.ezstack.ezapp.datastore.api.Rule.RuleStatus;

public class CuratorRulesManager implements RulesManager {

    private final static Logger LOG = LoggerFactory.getLogger(CuratorRulesManager.class);

    private final CuratorFramework _client;
    private final String _rulesPath;
    private final ObjectMapper _mapper;

    // outer table, inner table
    private volatile Supplier<ImmutableTable<String, String, Set<Rule>>> _activeRuleIndex;
    private final TreeCache _ruleCache;

    // <rule table name, rule itself>
    private volatile ConcurrentHashMap<String, Rule> _rules;

    @Inject
    public CuratorRulesManager(CuratorFramework client, @RulesPath String rulesPath) throws Exception {
        checkNotNull(client,"client");

        _client = client;
        _rulesPath = rulesPath;
        _mapper = new ObjectMapper();
        _rules = new ConcurrentHashMap<>();
        _activeRuleIndex = Suppliers.memoizeWithExpiration(this::getActiveRuleIndex, 10, TimeUnit.SECONDS);

        _ruleCache = new TreeCache(_client, _rulesPath);
        _ruleCache.getListenable().addListener(this::updateTableForEvent);
        _ruleCache.start();

    }

    private void handleAddOrUpdateEvent(TreeCacheEvent event) {
        ChildData childData = event.getData();
        int pathLength = ZKPaths.split(childData.getPath()).size();

        if (pathLength <= 1) {
            // This is the parent, it can be ignored
            return;
        }

        if (pathLength >= 3) {
            LOG.error("Invalid Znodes in rule directory in Zookeeper");
            LOG.error(childData.toString());
        }

        try {
            Rule rule = _mapper.readValue(childData.getData(), Rule.class);
            _rules.put(rule.getTable(), rule);
            LOG.info("Received update for rule with table {}. Current status is {}", rule.getTable(), rule.getStatus());
        } catch (IOException e) {
            LOG.error(e.toString());
            LOG.error("Rule cache is corrupted. Corrupt rules will be ignored");
        }
    }

    private ImmutableTable<String, String, Set<Rule>> getActiveRuleIndex() {
        LOG.info("Building rule index");
        Table<String, String, Set<Rule>> table = HashBasedTable.create();
        _rules.values()
                .stream()
                .filter(rule -> rule.getStatus() == Rule.RuleStatus.ACTIVE)
                .forEach(rule -> {
                    String innerKey = rule.getQuery().getJoin() != null ? rule.getQuery().getJoin().getTable() : Strings.EMPTY;
                    Set<Rule> applicableRules = firstNonNull(table.get(rule.getQuery().getTable(), innerKey), new HashSet<>());
                    applicableRules.add(rule);
                    table.put(rule.getQuery().getTable(), innerKey, applicableRules);
                });

        return table.cellSet()
                .stream()
                .collect(ImmutableTable.toImmutableTable(Table.Cell::getRowKey, Table.Cell::getColumnKey,
                        cell -> Collections.unmodifiableSet(firstNonNull(cell.getValue(), ImmutableSet.of()))));
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

    @Override
    public void create(Rule rule) throws RuleAlreadyExistsException {
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
            throw new RuntimeException("Failed to create rule");
        }
    }

    @Override
    public void remove(Rule rule) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Rule> getRules() {
        return ImmutableSet.copyOf(_rules.values());
    }

    @Override
    public Set<Rule> getRules(String outerTable, RuleStatus status) {
        if (status == RuleStatus.ACTIVE) {
            return _activeRuleIndex.get()
                    .row(outerTable)
                    .values()
                    .stream()
                    .flatMap(Set::stream)
                    .collect(ImmutableSet.toImmutableSet());
        }

        return _rules.values()
                .stream()
                .filter(rule -> rule.getQuery().getTable().equals(outerTable))
                .filter(rule -> rule.getStatus() == status)
                .collect(ImmutableSet.toImmutableSet());
    }

    @Override
    public Set<Rule> getRules(String outerTable, String innerTable, RuleStatus status) {
        if (status == RuleStatus.ACTIVE) {
            return _activeRuleIndex.get().get(outerTable, innerTable);
        }

        return _rules.values()
                .stream()
                .filter(rule -> rule.getQuery().getTable().equals(outerTable))
                .filter(rule -> rule.getQuery().getJoin() != null)
                .filter(rule -> rule.getQuery().getJoin().getTable().equals(innerTable))
                .filter(rule -> rule.getStatus() == status)
                .collect(ImmutableSet.toImmutableSet());
    }

    @Override
    public Set<Rule> getRules(RuleStatus status) {
        return _rules.values()
                .stream()
                .filter(rule -> rule.getStatus() == status)
                .collect(ImmutableSet.toImmutableSet());
    }

}