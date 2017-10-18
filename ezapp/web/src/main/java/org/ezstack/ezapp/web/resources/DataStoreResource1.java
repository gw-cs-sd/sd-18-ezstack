package org.ezstack.ezapp.web.resources;

import com.codahale.metrics.annotation.Timed;
import org.ezstack.ezapp.writer.api.DataWriter;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.Map;

@Path("sor/1")
@Produces(MediaType.APPLICATION_JSON)
public class DataStoreResource1 {

    private final DataWriter _dataWriter;

    public DataStoreResource1(DataWriter dataWriter) {
        _dataWriter = dataWriter;
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

}
