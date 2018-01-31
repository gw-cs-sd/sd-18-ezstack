package org.ezstack.ezapp.querybus.api;

import org.ezstack.ezapp.datastore.api.Query;

import java.util.concurrent.Future;

public interface QueryBusPublisher {

    void publishQuery(Query query, long responseTimeMs);

}
