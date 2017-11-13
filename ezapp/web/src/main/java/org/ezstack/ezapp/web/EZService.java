package org.ezstack.ezapp.web;

import com.codahale.metrics.servlets.PingServlet;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.ezstack.ezapp.web.resources.DataStoreResource1;
import org.ezstack.ezapp.web.resources.EZHealthCheck;
import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.ezstack.ezapp.datastore.api.DataWriter;

public class EZService extends Application<EZConfiguration> {

    private EZConfiguration _configuration;
    private Environment _environment;
    private Injector _injector;

    @Override
    public String getName() {
        return "EZapp";
    }

    @Override
    public void initialize(Bootstrap<EZConfiguration> bootstrap) {

    }

    @Override
    public void run(EZConfiguration configuration, Environment environment) {

        _configuration = configuration;
        _environment = environment;

        // add ping route
        environment.servlets().addServlet("/ping", new PingServlet());
        environment.healthChecks().register("placeholder", new EZHealthCheck());

        _injector = Guice.createInjector(new EZModule(_configuration, _environment));

        environment.jersey().register(new DataStoreResource1(_injector.getInstance(DataWriter.class)));

    }

    public static void main(String[] args) throws Exception {
        new EZService().run(args);
    }
}
