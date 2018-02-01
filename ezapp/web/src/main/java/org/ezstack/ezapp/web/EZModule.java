package org.ezstack.ezapp.web;

import com.google.inject.AbstractModule;
import io.dropwizard.setup.Environment;
import org.ezstack.ezapp.datastore.ReaderModule;
import org.ezstack.ezapp.datastore.WriterConfiguration;
import org.ezstack.ezapp.datastore.WriterModule;
import org.ezstack.ezapp.querybus.QueryBusModule;

public class EZModule extends AbstractModule {

    private final EZConfiguration _configuration;
    private final Environment _environment;

    public EZModule(EZConfiguration configuration, Environment environment) {
        _configuration = configuration;
        _environment = environment;
    }

    protected void configure() {
        install(new WriterModule(_configuration.getWriterConfiguration()));
        install(new ReaderModule(_configuration.getElasticSearchConfiguration()));
        install(new QueryBusModule(_configuration.getQueryBusConfiguration()));
    }
}
