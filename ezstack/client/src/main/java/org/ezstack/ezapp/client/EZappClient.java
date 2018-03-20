package org.ezstack.ezapp.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.ezstack.ezapp.datastore.api.*;
import org.ezstack.ezapp.web.api.WriteResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.ServiceUnavailableException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

public class EZappClient implements DataWriter, DataReader, RulesManager {

    private static final int DEFAULT_RETENTION_TIME_IN_MILLIS = 120000;
    private static final int DEFAULT_BATCH_SIZE = 100;

    private final static Logger LOG = LoggerFactory.getLogger(EZappClient.class);

    private static String SOR_PATH = "sor/1";

    private final URI _uri;
    private final Client _client;

    public EZappClient(String uri) {
        _uri = URI.create(uri);
        _client = ClientBuilder.newClient();
    }

    @Override
    public void createRule(Rule rule) throws RuleAlreadyExistsException {
        Response response =  _client
                .target(UriBuilder.fromUri(_uri).path(SOR_PATH).path("_rule"))
                .request(MediaType.APPLICATION_JSON)
                .post(Entity.entity(rule, MediaType.APPLICATION_JSON));

        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
            if (response.getStatus() == Response.Status.CONFLICT.getStatusCode()
                && RuleAlreadyExistsException.class.getName().equals(response.getHeaderString("X-EZ-Exception"))) {
                throw new RuleAlreadyExistsException(rule.getTable());
            }
            throw convertException(response);
        }
    }

    @Override
    public Set<Rule> getRules() {
        Response response =  _client
                .target(UriBuilder.fromUri(_uri).path(SOR_PATH).path("_rule"))
                .request(MediaType.APPLICATION_JSON)
                .get();

        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
            throw convertException(response);
        }

        return response.readEntity(new GenericType<Set<Rule>>() {});
    }

    @Override
    public void remove(Rule rule) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Rule> getRules(Rule.RuleStatus status) {
        Response response =  _client
                .target(UriBuilder.fromUri(_uri).path(SOR_PATH).path("_rule").path(status.toString()))
                .request(MediaType.APPLICATION_JSON)
                .get();

        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
            throw convertException(response);
        }

        return response.readEntity(new GenericType<Set<Rule>>() {});
    }

    @Override
    public Set<Rule> getRules(String outerTable, Rule.RuleStatus status) {
        checkArgument(Names.isLegalTableName(outerTable), "Invalid outer table name");

        Response response =  _client
                .target(UriBuilder.fromUri(_uri).path(SOR_PATH).path("_rule").path(status.toString()).path(outerTable))
                .request(MediaType.APPLICATION_JSON)
                .get();

        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
            throw convertException(response);
        }

        return response.readEntity(new GenericType<Set<Rule>>() {});
    }

    @Override
    public Set<Rule> getRules(String outerTable, String innerTable, Rule.RuleStatus status) {

        checkArgument(Names.isLegalTableName(outerTable), "Invalid outer table name");
        checkArgument(Names.isLegalTableName(innerTable), "Invalid inner table name");

        Response response =  _client
                .target(UriBuilder.fromUri(_uri).path(SOR_PATH).path("_rule").path(status.toString()).path(outerTable).path(innerTable))
                .request(MediaType.APPLICATION_JSON)
                .get();

        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
            throw convertException(response);
        }

        return response.readEntity(new GenericType<Set<Rule>>() {});
    }

    @Override
    public String create(String table, Map<String, Object> doc) {

        checkArgument(Names.isLegalTableName(table), "Invalid Table Name");

        Response response = _client
                .target(UriBuilder.fromUri(_uri).path(SOR_PATH).path(table))
                .request(MediaType.APPLICATION_JSON)
                .post(Entity.entity(doc, MediaType.APPLICATION_JSON));

        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
            throw convertException(response);
        }

        WriteResponse writeResponse = response.readEntity(WriteResponse.class);
        return writeResponse.getKey();
    }

    @Override
    public String create(String table, String key, Map<String, Object> doc) {

        checkArgument(Names.isLegalTableName(table), "Invalid Table Name");
        checkArgument(Names.isLegalKey(table), "Invalid Key");

        Response response = _client
                .target(UriBuilder.fromUri(_uri).path(SOR_PATH).path(table).path(key))
                .request(MediaType.APPLICATION_JSON)
                .post(Entity.entity(doc, MediaType.APPLICATION_JSON));

        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
            throw convertException(response);
        }

        WriteResponse writeResponse = response.readEntity(WriteResponse.class);
        return writeResponse.getKey();
    }

    public String update(String table, String key, Map<String, Object> doc) {
        Response response = _client
                .target(UriBuilder.fromUri(_uri).path(SOR_PATH).path(table))
                .request(MediaType.APPLICATION_JSON)
                .put(Entity.entity(doc, MediaType.APPLICATION_JSON));

        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
            throw convertException(response);
        }

        WriteResponse writeResponse = response.readEntity(WriteResponse.class);
        return writeResponse.getKey();
    }

    // TODO: refactor the datawriter to have a bulk function, then refactor this function to implement it
    private Response bulkWrite(List<BulkDocument> bulkDocuments) {
        return _client
                .target(UriBuilder.fromUri(_uri).path(SOR_PATH).path("_bulk"))
                .request(MediaType.APPLICATION_JSON)
                .post(Entity.entity(bulkDocuments, MediaType.APPLICATION_JSON));
    }

    public Map<String, Object> getDocument(String table, String key) {
        Response response = _client
                .target(UriBuilder.fromUri(_uri).path(SOR_PATH).path(table).path(key))
                .request(MediaType.APPLICATION_JSON)
                .get();

        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
            throw convertException(response);
        }

        return response.readEntity(new GenericType<Map<String, Object>>() {});

    }

    @Override
    public QueryResult getDocuments(long scrollInMillis, Query query) {
        return getDocuments(scrollInMillis, DEFAULT_BATCH_SIZE, query);
    }

    @Override
    public QueryResult getDocuments(int batchSize, Query query) {
        return getDocuments(DEFAULT_RETENTION_TIME_IN_MILLIS, batchSize, query);
    }

    @Override
    public QueryResult getDocuments(Query query) {
        return getDocuments(DEFAULT_RETENTION_TIME_IN_MILLIS, DEFAULT_BATCH_SIZE, query);
    }

    public QueryResult getDocuments(long scrollInMillis, int batchSize, Query query) {
        Response response =  _client
                .target(UriBuilder.fromUri(_uri).path(SOR_PATH).path("_search"))
                .request(MediaType.APPLICATION_JSON)
                .property("scrolInMillis", scrollInMillis)
                .property("batchSize", batchSize)
                .post(Entity.entity(query, MediaType.APPLICATION_JSON));

        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
            throw new RuntimeException(response.getEntity().toString());
        }

        return response.readEntity(QueryResult.class);

    }

    private RuntimeException convertException(Response response) {
        String exceptionType = response.getHeaderString("X-EZ-Exception");

        if (response.getStatus() == Response.Status.BAD_REQUEST.getStatusCode()) {
            if (IllegalArgumentException.class.getName().equals(exceptionType)) {
                return new IllegalArgumentException(response.readEntity(String.class));
            } else if (JsonProcessingException.class.getName().equals(exceptionType)) {
                return new RuntimeException(new JsonProcessingException(response.readEntity(String.class)) {});
            }
        } else if (response.getStatus() == Response.Status.SERVICE_UNAVAILABLE.getStatusCode() &&
                ServiceUnavailableException.class.getName().equals(exceptionType)) {
            if (response.hasEntity()) {
                return response.readEntity(ServiceUnavailableException.class);
            } else {
                return new ServiceUnavailableException();
            }
        }

        return new RuntimeException(response.readEntity(String.class));
    }

}
