package org.ezstack.ezapp.web.resources;

import com.codahale.metrics.annotation.Timed;
import org.ezstack.ezapp.datastore.api.*;
import org.ezstack.ezapp.querybus.api.QueryBusPublisher;
import org.ezstack.ezapp.web.api.response.BulkResponse;
import org.ezstack.ezapp.web.api.response.SuccessResponse;
import org.ezstack.ezapp.web.api.response.WriteResponse;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.*;

/**
 * Jersey resource for accessing EZapp's {@link DataWriter}, {@link DataReader}, and {@link RulesManager}.
 *
 * <p>If a 4xx error is thrown by the resource, a header with the exeception will be defined under "X-EZ-Exception".
 */

@Path("sor/1")
@Produces(MediaType.APPLICATION_JSON)
public class DataStoreResource1 {

    private final DataWriter _dataWriter;
    private final DataReader _dataReader;
    private final QueryBusPublisher _queryBusPublisher;
    private final RulesManager _rulesManager;

    public DataStoreResource1(DataWriter dataWriter, DataReader dataReader, QueryBusPublisher queryBusPublisher,
                              RulesManager rulesManager) {
        _dataWriter = dataWriter;
        _dataReader = dataReader;
        _queryBusPublisher = queryBusPublisher;
        _rulesManager = rulesManager;
    }

    @POST
    @Path("{table}")
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public WriteResponse create(@PathParam("table") String table,
                                Map<String, Object> json) {
        return new WriteResponse(_dataWriter.create(table, json));
    }

    @POST
    @Path("{table}/{key}")
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public WriteResponse create(@PathParam("table") String table,
                                @PathParam("key") String key,
                                Map<String, Object> json) {
        return new WriteResponse(_dataWriter.create(table, key, json));
    }

    @PUT
    @Path("{table}/{key}")
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public WriteResponse update(@PathParam("table") String table,
                                @PathParam("key") String key,
                                Map<String, Object> json) {
        return new WriteResponse(_dataWriter.update(table, key, json));
    }

    @GET
    @Path("{table}/{key}")
    @Timed
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Object> getDocument(@PathParam("table") String table,
                                           @PathParam("key") String key) {
        Optional<Map<String, Object>> ret = Optional.ofNullable(_dataReader.getDocument(table, key));
        return ret.orElse(Collections.emptyMap());
    }

    @POST
    @Path("_search/")
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public QueryResult search(@QueryParam("retentionTimeInMillis") @DefaultValue("120000") long retentionTimeInMillis,
                              @QueryParam("batchSize") @DefaultValue("100") int batchSize,
                              Query query) {
        long timeStart = System.currentTimeMillis();
        QueryResult ret = _dataReader.getDocuments(retentionTimeInMillis, batchSize, query);
        _queryBusPublisher.publishQueryAsync(query, System.currentTimeMillis() - timeStart);
        return ret;
    }

    @POST
    @Path("_bulk/")
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public BulkResponse bulkWrite(List<BulkDocument> bulkDocuments) {
        BulkResponse bulkResponse = new BulkResponse();

        for (BulkDocument doc : bulkDocuments) {
            switch (doc.getOpType()) {
                case CREATE:
                    try {
                        if (doc.getKey() == null || doc.getKey().isEmpty()) {
                            bulkResponse.addItem(create(doc.getTable(), doc.getDocument()));
                        } else {
                            bulkResponse.addItem(create(doc.getTable(), doc.getKey(), doc.getDocument()));
                        }
                    } catch (Exception e) {
                        bulkResponse.addToErrorCount();
                        bulkResponse.addErrorMessage(
                                BulkResponse.createGenericErrorMessage("failed to create document", doc));
                    }

                    break;
                case UPDATE:
                    try {
                        if (doc.getKey() == null || doc.getKey().isEmpty()) {
                            bulkResponse.addToErrorCount();
                            bulkResponse.addErrorMessage(
                                    BulkResponse.createGenericErrorMessage("failed to update document due to missing key", doc));
                        } else {
                            bulkResponse.addItem(update(doc.getTable(), doc.getKey(), doc.getDocument()));
                        }
                    } catch (Exception e) {
                        bulkResponse.addToErrorCount();
                        bulkResponse.addErrorMessage(
                                BulkResponse.createGenericErrorMessage("failed to update document", doc));
                    }
                    break;
                default:
                    bulkResponse.addToErrorCount();
                    bulkResponse.addErrorMessage(
                            BulkResponse.createGenericErrorMessage("unkown bulk operation", doc));
                    break;
            }
        }

        return bulkResponse;
    }

    @POST
    @Path("_rule/")
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public SuccessResponse createRule(Rule rule) throws RuleAlreadyExistsException {
        _rulesManager.createRule(rule);
        return SuccessResponse.instance();
    }

    @PUT
    @Path("_rule/_table/{ruleTable}")
    @Timed
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.APPLICATION_JSON)
    public SuccessResponse setRuleStatus(@PathParam("ruleTable") String ruleTable,
                                         String status) {
        _rulesManager.setRuleStatus(ruleTable, Rule.RuleStatus.valueOf(status.toUpperCase()));
        return SuccessResponse.instance();
    }

    @GET
    @Path("_rule/_table/{ruleTable}")
    @Timed
    @Produces(MediaType.APPLICATION_JSON)
    public Rule getRule(@PathParam("ruleTable") String ruleTable) {
        Rule rule = _rulesManager.getRule(ruleTable);
        if (rule == null) {
            throw new NotFoundException("Rule does not exist");
        }
        return rule;
    }

    @GET
    @Path("_rule/")
    @Timed
    @Produces(MediaType.APPLICATION_JSON)
    public Set<Rule> getRules() {
        return _rulesManager.getRules();
    }

    @GET
    @Path("_rule/{status}")
    @Timed
    @Produces(MediaType.APPLICATION_JSON)
    public Set<Rule> getRules(@PathParam("status") Rule.RuleStatus status) {
        return _rulesManager.getRules(status);
    }


    @GET
    @Path("_rule/{status}/{outerTable}")
    @Timed
    @Produces(MediaType.APPLICATION_JSON)
    public Set<Rule> getRules(@PathParam("status") Rule.RuleStatus status,
                              @PathParam(("outerTable")) String outerTable) {
        return _rulesManager.getRules(outerTable, status);
    }

    @GET
    @Path("_rule/{status}/{outerTable}/{innerTable}")
    @Timed
    @Produces(MediaType.APPLICATION_JSON)
    public Set<Rule> getRules(@PathParam("status") Rule.RuleStatus status,
                              @PathParam("outerTable") String outerTable,
                              @PathParam("innerTable") String innerTable) {
        return _rulesManager.getRules(outerTable, innerTable, status);
    }

}
