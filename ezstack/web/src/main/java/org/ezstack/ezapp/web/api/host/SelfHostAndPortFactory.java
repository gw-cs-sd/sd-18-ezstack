package org.ezstack.ezapp.web.api.host;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.net.HostAndPort;
import io.dropwizard.jetty.ConnectorFactory;
import io.dropwizard.jetty.HttpConnectorFactory;
import io.dropwizard.server.DefaultServerFactory;
import io.dropwizard.server.ServerFactory;
import io.dropwizard.server.SimpleServerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

public class SelfHostAndPortFactory {

    public static HostAndPort getSelfHostAndPort(ServerFactory serverFactory) {
        // Our method for obtaining connector factories from the server factory varies depending on the latter's type
        List<ConnectorFactory> appConnectorFactories;
        if (serverFactory instanceof DefaultServerFactory) {
            appConnectorFactories = ((DefaultServerFactory) serverFactory).getApplicationConnectors();
        } else if (serverFactory instanceof SimpleServerFactory) {
            appConnectorFactories = Collections.singletonList(((SimpleServerFactory) serverFactory).getConnector());
        } else {
            throw new IllegalStateException("Encountered an unexpected ServerFactory type");
        }

        return getHostAndPortFromConnectorFactories(appConnectorFactories);
    }

    private static HostAndPort getHostAndPortFromConnectorFactories(List<ConnectorFactory> connectors) {
        // find the first connector that matches and return it host/port information (in practice there should
        // be one, and just one, match)
        try {
            HttpConnectorFactory httpConnectorFactory = (HttpConnectorFactory) Iterables.find(connectors, Predicates.instanceOf(HttpConnectorFactory.class));

            String host = httpConnectorFactory.getBindHost();
            if (host == null) {
                host = getLocalHost().getHostAddress();
            }
            int port = httpConnectorFactory.getPort();
            return HostAndPort.fromParts(host, port);
        } catch (NoSuchElementException ex) {
            throw new IllegalStateException("Did not find a valid HttpConnector for the server", ex);
        }
    }

    private static InetAddress getLocalHost() {
        try {
            return InetAddress.getLocalHost();
        } catch (IOException e) {
            throw new AssertionError(e); // Should never happen
        }
    }
}
