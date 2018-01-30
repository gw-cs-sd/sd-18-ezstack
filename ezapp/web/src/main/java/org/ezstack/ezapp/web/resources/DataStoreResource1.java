package org.ezstack.ezapp.web.resources;

import com.codahale.metrics.annotation.Timed;
import org.ezstack.ezapp.datastore.api.*;

import javax.ws.rs.Consumes;
import javax.ws.rs.Produces;
import javax.ws.rs.PathParam;
import javax.ws.rs.Path;
import javax.ws.rs.POST;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.QueryParam;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.core.MediaType;
import java.util.UUID;
import java.util.Collections;
import java.util.Map;
import java.util.List;
import java.util.Optional;

@Path("sor/1")
@Produces(MediaType.APPLICATION_JSON)
public class DataStoreResource1 {

    private final DataWriter _dataWriter;
    private final DataReader _dataReader;

    public DataStoreResource1(DataWriter dataWriter, DataReader dataReader) {
        _dataWriter = dataWriter;
        _dataReader = dataReader;
    }

    @POST
    @Path("{table}")
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public WriteResponse create(@PathParam("table") String table,
                                  Map<String, Object> json) {
        return create(table, UUID.randomUUID().toString(), json);
    }

    @POST
    @Path ("{table}/{key}")
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public WriteResponse create(@PathParam("table") String table,
                                  @PathParam("key") String key,
                                  Map<String, Object> json) {
        _dataWriter.create(table, key, json);
        return new WriteResponse(key);
    }

    @PUT
    @Path("{table}/{key}")
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public WriteResponse update(@PathParam("table") String table,
                                  @PathParam("key") String key,
                                  Map<String, Object> json) {
        _dataWriter.update(table, key, json);
        return new WriteResponse(key);
    }

    @GET
    @Path("{table}/{key}")
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
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
    public List<Map<String, Object>> search(@QueryParam("scroll") @DefaultValue("120000") long scrollInMillis,
                                            @QueryParam("batchSize") @DefaultValue("100") int batchSize,
                                            Query query) {
        long currentTimeInMs = System.currentTimeMillis();
        List<Map<String, Object>> ret = _dataReader.getDocuments(scrollInMillis, batchSize, query);
        // new QueryMetaData(query, System.currentTimeMillis()-currentTimeInMs); // send it to quag for analysis
        return ret;
    }

    @POST
    @Path("_bulk/")
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public BulkResponse bulkWrite(List<BulkDocument> bulkDocuments) {
        BulkResponse bulkResponse = new BulkResponse();

        for (BulkDocument doc: bulkDocuments) {
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
}
