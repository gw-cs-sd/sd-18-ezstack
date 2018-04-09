package org.ezstack.ezapp.rules.core;

import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import org.ezstack.ezapp.common.guice.SelfHostAndPort;
import org.ezstack.ezapp.common.lifecycle.GuavaManagedService;
import org.ezstack.ezapp.common.lifecycle.LifeCycleRegistry;
import org.ezstack.ezapp.datastore.api.RulesManager;

import java.util.concurrent.TimeUnit;

public class RulesMonitor extends LeaderService {

    private static final String SERVICE_NAME = "rules-monitor";
    private static final String LEADER_DIR = "/leader/monitor";

    @Inject
    public RulesMonitor(CuratorFactory curatorFactory, LifeCycleRegistry lifecycle, RulesManager rulesManager,
                        @SelfHostAndPort HostAndPort hostAndPort) {
        super(curatorFactory, LEADER_DIR, hostAndPort.toString(), SERVICE_NAME,
                1, TimeUnit.MINUTES, () -> new LocalRulesMonitor(rulesManager, curatorFactory, "localhost:9092"));

        lifecycle.manage(new GuavaManagedService(this));
    }
}
