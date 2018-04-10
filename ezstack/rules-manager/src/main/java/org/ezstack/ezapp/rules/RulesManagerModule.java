package org.ezstack.ezapp.rules;

import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Names;
import org.ezstack.ezapp.common.lifecycle.GuavaManagedService;
import org.ezstack.ezapp.common.lifecycle.LifeCycleRegistry;
import org.ezstack.ezapp.datastore.api.RulesManager;
import org.ezstack.ezapp.rules.api.BootstrapperPath;
import org.ezstack.ezapp.rules.api.ConsistentRulesManager;
import org.ezstack.ezapp.rules.api.RulesPath;
import org.ezstack.ezapp.rules.core.CuratorFactory;
import org.ezstack.ezapp.rules.core.CachingCuratorRulesManager;
import org.ezstack.ezapp.rules.core.CuratorRulesManager;
import org.ezstack.ezapp.rules.core.RulesMonitor;

public class RulesManagerModule extends PrivateModule {

    private static final String RULES_PATH = "/rules";
    private static final String BOOSTRAPPER_PATH = "/bootstrapper";

    private final RulesManagerConfiguration _configuration;

    public RulesManagerModule(RulesManagerConfiguration configuration) {
        _configuration = configuration;
    }

    @Override
    protected void configure() {
        bind(Integer.class).annotatedWith(Names.named("partitionCount")).toInstance(_configuration.getPartitionCount());
        bind(Integer.class).annotatedWith(Names.named("replicationFactor")).toInstance(_configuration.getReplicationFactor());
        bind(String.class).annotatedWith(Names.named("bootstrapTopicName")).toInstance(_configuration.getBootstrapTopicName());
        bind(String.class).annotatedWith(Names.named("shutdownTopicName")).toInstance(_configuration.getShutdownTopicName());
        bind(String.class).annotatedWith(Names.named("kafkaBootstrapServers")).toInstance(_configuration.getKafkaBootstrapServers());
        bind(String.class).annotatedWith(RulesPath.class).toInstance(RULES_PATH);
        bind(String.class).annotatedWith(BootstrapperPath.class).toInstance(BOOSTRAPPER_PATH);
        bind(String.class).annotatedWith(Names.named("zookeeperHosts")).toInstance(_configuration.getZookeeperHosts());
        bind(CuratorFactory.class).asEagerSingleton();
        bind(RulesMonitor.class).asEagerSingleton();

        expose(RulesManager.class);
    }

    @Singleton
    @Provides
    RulesManager provideRulesManager(@RulesPath String rulesPath, CuratorFactory curatorFactory,
                                     @ConsistentRulesManager RulesManager consistentRulesManager,
                                     LifeCycleRegistry lifeCycleRegistry) {
        CachingCuratorRulesManager rulesManager = new CachingCuratorRulesManager(rulesPath, curatorFactory,
                consistentRulesManager);
        lifeCycleRegistry.manage(new GuavaManagedService(rulesManager));
        return rulesManager;
    }

    @Singleton
    @ConsistentRulesManager
    @Provides
    RulesManager provideConsistentRulesManager(@RulesPath String rulesPath, CuratorFactory curatorFactory,
                                     LifeCycleRegistry lifeCycleRegistry) {
        CuratorRulesManager rulesManager = new CuratorRulesManager(rulesPath, curatorFactory);
        lifeCycleRegistry.manage(new GuavaManagedService(rulesManager));
        return rulesManager;
    }
}
