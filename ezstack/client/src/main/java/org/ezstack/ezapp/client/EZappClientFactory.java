package org.ezstack.ezapp.client;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;

public class EZappClientFactory {

    public static EZappClient newEzappClient(String uri) {
        return new EZappClient(uri);
    }
}
