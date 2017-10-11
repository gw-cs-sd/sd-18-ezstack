package org.ezstack.ezapp.web.resources;

import com.codahale.metrics.annotation.Timed;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

@Path("sor/1")
@Produces(MediaType.APPLICATION_JSON)
public class DataStoreResource1 {


    @POST
    @Path ("{table}/{key}")
    @Timed
//    @Consumes(MediaType.APPLICATION_JSON)
    public SuccessResponse create(@PathParam("table") String table, @PathParam("key") String key) {
        return SuccessResponse.instance();
    }

}
