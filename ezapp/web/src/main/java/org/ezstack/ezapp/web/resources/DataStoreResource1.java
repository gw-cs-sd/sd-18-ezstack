package org.ezstack.ezapp.web.resources;

import com.codahale.metrics.annotation.Timed;
import org.ezstack.ezapp.datastore.api.DataReader;
import org.ezstack.ezapp.datastore.api.DataWriter;
import org.ezstack.ezapp.datastore.api.Query;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Produces;
import javax.ws.rs.Consumes;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.MediaType;
import java.util.HashMap;
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
    @Path ("{table}/{key}")
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    public SuccessResponse create(@PathParam("table") String table,
                                  @PathParam("key") String key,
                                  Map<String, Object> json) {
        _dataWriter.create(table, key, json);
        return SuccessResponse.instance();
    }

    @PUT
    @Path("{table}/{key}")
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    public SuccessResponse update(@PathParam("table") String table,
                                  @PathParam("key") String key,
                                  Map<String, Object> json) {
        _dataWriter.update(table, key, json);
        return SuccessResponse.instance();
    }

    @GET
    @Path("{table}/{key}")
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Object> getDocument(@PathParam("table") String table,
                                           @PathParam("key") String id) {
        Optional<Map<String, Object>> ret = Optional.ofNullable(_dataReader.getDocument(table, id));
        return ret.orElse(new HashMap<>());
    }

    @POST
    @Path("search/")
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public List<Map<String, Object>> search(Map<String, Object> json) {
        return _dataReader.getDocuments(new Query(json));
    }

}
