package org.ezstack.ezapp.rules.core;

import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.ezstack.ezapp.common.guice.SelfHostAndPort;
import org.ezstack.ezapp.common.lifecycle.GuavaManagedService;
import org.ezstack.ezapp.common.lifecycle.LifeCycleRegistry;
import org.ezstack.ezapp.datastore.api.RulesManager;
import org.ezstack.ezapp.jobmanager.api.JobManager;
import org.ezstack.ezapp.rules.api.BootstrapperPath;
import org.ezstack.ezapp.rules.api.ConsistentRulesManager;
import org.ezstack.ezapp.rules.api.RulesPath;
import org.ezstack.ezapp.rules.config.BootstrapperConfig;

import java.util.concurrent.TimeUnit;

public class RulesMonitor extends LeaderService {

    private static final String SERVICE_NAME = "rules-monitor";
    private static final String LEADER_DIR = "/leader/monitor";

    @Inject
    public RulesMonitor(CuratorFactory curatorFactory, LifeCycleRegistry lifecycle,
                        @ConsistentRulesManager RulesManager rulesManager, @SelfHostAndPort HostAndPort hostAndPort,
                        @Named("partitionCount") int partitionCount, @Named("replicationFactor") int replicationFactor,
                        @Named("kafkaBootstrapServers") String bootstrapServers, @Named("bootstrapTopicName") String bootstrapTopicName,
                        @Named("shutdownTopicName") String shutdownTopicName, @RulesPath String rulesPath,
                        @BootstrapperPath String bootstrapperPath,
                        BootstrapperConfig bootstrapperConfig,
                        JobManager jobManager) {
        super(curatorFactory, LEADER_DIR, hostAndPort.toString(), SERVICE_NAME,
                1, TimeUnit.MINUTES, () -> new LocalRulesMonitor(rulesManager, curatorFactory,
                        bootstrapServers, partitionCount, replicationFactor, bootstrapTopicName, shutdownTopicName,
                        rulesPath, bootstrapperPath, bootstrapperConfig, jobManager));

        lifecycle.manage(new GuavaManagedService(this));
    }
}
