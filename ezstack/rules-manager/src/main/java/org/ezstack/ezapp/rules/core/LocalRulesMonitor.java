package org.ezstack.ezapp.rules.core;

import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;
import org.ezstack.ezapp.datastore.api.Rule;
import org.ezstack.ezapp.datastore.api.RulesManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class LocalRulesMonitor extends AbstractService {

    private final static Logger LOG = LoggerFactory.getLogger(LocalRulesMonitor.class);

    private final RulesManager _rulesManager;
    private final CuratorFactory _curatorFactory;

    private CuratorFramework _curator;

    private ScheduledExecutorService _service;

    public LocalRulesMonitor(RulesManager rulesManager, CuratorFactory curatorFactory) {
        _rulesManager = rulesManager;
        _curatorFactory = curatorFactory;
    }

    @Override
    protected void doStart() {
        try {
            _curator = _curatorFactory.getStartedCuratorFramework();
        } catch (Exception e) {
            notifyFailed(e);
            throw e;
        }

        _service = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("rules-monitor-%d").build());

        _service.scheduleAtFixedRate(this::bootstrapPendingRulesIfFullyAcknowledged, 30, 30, TimeUnit.SECONDS);

        notifyStarted();
    }

    private void bootstrapPendingRulesIfFullyAcknowledged() {
        Set<Rule> pendingRules = _rulesManager.getRules(Rule.RuleStatus.PENDING);
        pendingRules.parallelStream()
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

    @Override
    protected void doStop() {
        try {
            _curator.close();
        } catch (Exception e) {
            notifyFailed(e);
            throw e;
        }

        notifyStopped();
        notify();
    }


}
