package org.ezstack.ezapp.web.exceptionmappers;

import io.dropwizard.jersey.errors.ErrorMessage;
import org.ezstack.ezapp.datastore.api.RuleAlreadyExistsException;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class RuleAlreadyExistsExceptionMapper implements ExceptionMapper<RuleAlreadyExistsException> {
    @Override
    public Response toResponse(RuleAlreadyExistsException e) {
            return Response.status(Response.Status.CONFLICT)
                    .header("X-EZ-Exception", RuleAlreadyExistsException.class.getName())
                    .entity(new ErrorMessage(Response.Status.CONFLICT.getStatusCode(),
                            e.getMessage()))
                    .type(MediaType.APPLICATION_JSON_TYPE)
                    .build();
    }
}
