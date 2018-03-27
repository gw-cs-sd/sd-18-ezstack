package org.ezstack.ezapp.web;

import com.google.inject.AbstractModule;
import io.dropwizard.setup.Environment;
import org.ezstack.ezapp.common.lifecycle.LifeCycleRegistry;
import org.ezstack.ezapp.datastore.ReaderModule;
import org.ezstack.ezapp.datastore.WriterModule;
import org.ezstack.ezapp.querybus.QueryBusModule;
import org.ezstack.ezapp.rules.RulesManagerModule;
import org.ezstack.ezapp.web.api.lifecycle.DropwizardLifeCycleRegistry;

public class EZModule extends AbstractModule {

    private final EZConfiguration _configuration;
    private final Environment _environment;

    public EZModule(EZConfiguration configuration, Environment environment) {
        _configuration = configuration;
        _environment = environment;
    }

    protected void configure() {

        bind(Environment.class).toInstance(_environment);
        bind(LifeCycleRegistry.class).to(DropwizardLifeCycleRegistry.class).asEagerSingleton();

        install(new WriterModule(_configuration.getWriterConfiguration()));
        install(new ReaderModule(_configuration.getElasticSearchConfiguration()));
        install(new QueryBusModule(_configuration.getQueryBusConfiguration()));
        install(new RulesManagerModule(_configuration.getRulesManagerConfiguration()));
    }
}
