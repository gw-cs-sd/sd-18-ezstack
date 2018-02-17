package org.ezstack.ezapp.web;

import com.codahale.metrics.servlets.PingServlet;
import com.datastax.driver.core.utils.UUIDs;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.ezstack.ezapp.datastore.api.DataReader;
import org.ezstack.ezapp.querybus.api.QueryBusPublisher;
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
        // Quick call to UUID in order to allow the system to call getPid() before the first request
        // This is nearly instantaneous in some systems, which makes it harmless to do.
        // On other system's however, this call can take several seconds. Because of the delay, it makes sense
        // to do it on startup so that the first query does not get delayed by this call
        UUIDs.timeBased();
    }

    @Override
    public void run(EZConfiguration configuration, Environment environment) {

        _configuration = configuration;
        _environment = environment;

        // add ping route
        environment.servlets().addServlet("/ping", new PingServlet());
        environment.healthChecks().register("placeholder", new EZHealthCheck());

        _injector = Guice.createInjector(new EZModule(_configuration, _environment));

        environment.jersey().register(new DataStoreResource1(_injector.getInstance(DataWriter.class),
                _injector.getInstance(DataReader.class), _injector.getInstance(QueryBusPublisher.class)));

    }

    public static void main(String[] args) throws Exception {
        new EZService().run(args);
    }
}
