package org.ezstack.ezapp.rules;

import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.ezstack.ezapp.datastore.api.RulesManager;
import org.ezstack.ezapp.rules.api.RulesPath;
import org.ezstack.ezapp.rules.core.CuratorRulesManager;

public class RulesManagerModule extends PrivateModule {

    private static final String RULES_PATH = "/rules";

    private final RulesManagerConfiguration _configuration;

    public RulesManagerModule(RulesManagerConfiguration configuration) {
        _configuration = configuration;
    }

    @Override
    protected void configure() {
        bind(RulesManager.class).to(CuratorRulesManager.class).asEagerSingleton();
        bind(String.class).annotatedWith(RulesPath.class).toInstance(RULES_PATH);
        expose(RulesManager.class);
    }

    @Singleton
    @Provides
    CuratorFramework provideCuratorFramework() {
        CuratorFramework client = CuratorFrameworkFactory.newClient(_configuration.getZookeeperHosts(),
                new ExponentialBackoffRetry(1000, 3));
        client.start();
        return client;
    }
}
