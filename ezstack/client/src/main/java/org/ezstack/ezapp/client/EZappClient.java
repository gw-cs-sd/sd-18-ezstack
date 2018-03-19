package org.ezstack.ezapp.client;

import org.apache.http.client.utils.URIBuilder;
import org.ezstack.ezapp.datastore.api.BulkDocument;
import org.ezstack.ezapp.datastore.api.Query;
import org.ezstack.ezapp.datastore.api.Rule;
import org.ezstack.ezapp.datastore.api.RuleAlreadyExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.util.List;
import java.util.Map;

public class EZappClient implements {

    private final static Logger LOG = LoggerFactory.getLogger(EZappClient.class);

    private static String SOR_PATH = "sor/1";

    private final URI _uri;
    private final Client _client;

    public EZappClient(String uri) {
        _uri = URI.create(uri);
        _client = ClientBuilder.newClient();
    }

    public Response createRule(Rule rule) throws RuleAlreadyExistsException {
        return _client
                .target(UriBuilder.fromUri(_uri).path(SOR_PATH).path("_rule"))
                .request(MediaType.APPLICATION_JSON)
                .post(Entity.entity(rule, MediaType.APPLICATION_JSON));

    }

    public Response createDocument(String table, Map<String, Object> doc) {
        return _client
                .target(UriBuilder.fromUri(_uri).path(SOR_PATH).path(table))
                .request(MediaType.APPLICATION_JSON)
                .post(Entity.entity(doc, MediaType.APPLICATION_JSON));
    }

    public Response createDocument(String table, String key, Map<String, Object> doc) {
        return _client
                .target(UriBuilder.fromUri(_uri).path(SOR_PATH).path(table).path(key))
                .request(MediaType.APPLICATION_JSON)
                .post(Entity.entity(doc, MediaType.APPLICATION_JSON));
    }

    public Response updateDocument(String table, String key, Map<String, Object> doc) {
        return _client
                .target(UriBuilder.fromUri(_uri).path(SOR_PATH).path(table))
                .request(MediaType.APPLICATION_JSON)
                .put(Entity.entity(doc, MediaType.APPLICATION_JSON));
    }

    public Response bulkWrite(List<BulkDocument> bulkDocuments) {
        return _client
                .target(UriBuilder.fromUri(_uri).path(SOR_PATH).path("_bulk"))
                .request(MediaType.APPLICATION_JSON)
                .post(Entity.entity(bulkDocuments, MediaType.APPLICATION_JSON));
    }

    public Response getDocument(String table, String key) {
        return _client
                .target(UriBuilder.fromUri(_uri).path(SOR_PATH).path(table).path(key))
                .request(MediaType.APPLICATION_JSON)
                .get();
    }

    public Response search(long scrollInMillis, Query query) {
        return search(scrollInMillis, 100, query);
    }

    public Response search(int batchSize, Query query) {
        return search(120000, batchSize, query);
    }

    public Response search(Query query) {
        return search(120000, 100, query);
    }

    public Response search(long scrollInMillis, int batchSize, Query query) {
        return _client
                .target(UriBuilder.fromUri(_uri).path(SOR_PATH).path("_search"))
                .request(MediaType.APPLICATION_JSON)
                .property("scrolInMillis", scrollInMillis)
                .property("batchSize", batchSize)
                .post(Entity.entity(query, MediaType.APPLICATION_JSON));
    }
}
