package org.ezstack.ezapp.client;

import org.ezstack.ezapp.datastore.api.DataReader;
import org.ezstack.ezapp.datastore.api.DataWriter;
import org.ezstack.ezapp.datastore.api.RulesManager;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;

public class EZappClientFactory {

    public static EZappClient newEzappClient(String uri) {
        return new EZappClient(uri);
    }

    public static DataWriter newDataWriter(String uri) {
        return newEzappClient(uri);
    }

    public static DataReader newDataReader(String uri) {
        return newEzappClient(uri);
    }

    public static RulesManager newRulesManager(String uri) {
        return newEzappClient(uri);
    }
}
