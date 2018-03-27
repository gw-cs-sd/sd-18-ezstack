package org.ezstack.ezapp.rules;

import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import org.ezstack.ezapp.common.lifecycle.GuavaManagedService;
import org.ezstack.ezapp.common.lifecycle.LifeCycleRegistry;
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
        bind(String.class).annotatedWith(RulesPath.class).toInstance(RULES_PATH);
        bind(String.class).annotatedWith(Names.named("zookeeperHosts")).toInstance(_configuration.getZookeeperHosts());
        expose(RulesManager.class);
    }

    @Singleton
    @Provides
    RulesManager provideRulesManager(@RulesPath String rulesPath, @Named("zookeeperHosts") String zookeeperHosts,
                                     LifeCycleRegistry lifeCycleRegistry) {
        CuratorRulesManager rulesManager = new CuratorRulesManager(rulesPath, zookeeperHosts);
        lifeCycleRegistry.manage(new GuavaManagedService(rulesManager));
        return rulesManager;
    }
}
