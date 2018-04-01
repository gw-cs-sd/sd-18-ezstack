package org.ezstack.ezapp.rules.core;

import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.curator.framework.CuratorFramework;
import org.ezstack.ezapp.datastore.api.Rule;
import org.ezstack.ezapp.datastore.api.RulesManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

        _service.scheduleAtFixedRate(() -> {

            Set<Rule> pendingRules = _rulesManager.getRules(Rule.RuleStatus.PENDING);
            LOG.info(pendingRules.toString());


        }, 30, 30, TimeUnit.SECONDS);

        notifyStarted();
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
