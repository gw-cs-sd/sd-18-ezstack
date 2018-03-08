package org.ezstack.ezapp.web.exceptionmappers;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.exc.InvalidDefinitionException;
import com.google.common.base.MoreObjects;
import io.dropwizard.jersey.errors.ErrorMessage;
import io.dropwizard.jersey.errors.LoggingExceptionMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;

@Provider
public class JsonProcessingExceptionMapper extends LoggingExceptionMapper<JsonProcessingException> {
    private static final Logger LOG = LoggerFactory.getLogger(JsonProcessingExceptionMapper.class);
    private static final IllegalArgumentExceptionMapper illegalArgumentExceptionMapper = new IllegalArgumentExceptionMapper();
    private final boolean showDetails;

    public JsonProcessingExceptionMapper() {
        this(false);
    }

    public JsonProcessingExceptionMapper(boolean showDetails) {
        this.showDetails = showDetails;
    }

    public boolean isShowDetails() {
        return showDetails;
    }

    @Override
    public Response toResponse(JsonProcessingException exception) {
        /*
         * If the error is in the JSON generation or an invalid definition, it's a server error.
         */

        if (exception instanceof InvalidDefinitionException && exception.getCause() instanceof IllegalArgumentException) {
            return illegalArgumentExceptionMapper.toResponse((IllegalArgumentException) exception.getCause());
        }

        if (exception instanceof InvalidDefinitionException && exception.getCause() instanceof NullPointerException) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .header("X-EZ-Exception", NullPointerException.class.getName())
                    .entity(new ErrorMessage(Response.Status.BAD_REQUEST.getStatusCode(),
                            MoreObjects.firstNonNull(exception.getOriginalMessage(), "Invalid JSON Object")))
                    .type(MediaType.APPLICATION_JSON_TYPE)
                    .build();
        }

        if (exception instanceof JsonGenerationException || exception instanceof InvalidDefinitionException) {
            return super.toResponse(exception); // LoggingExceptionMapper will log exception
        }

        /*
         * Otherwise, it's those pesky users.
         */
        LOG.debug("Unable to process JSON", exception);

        final String message = exception.getOriginalMessage();
        final ErrorMessage errorMessage = new ErrorMessage(Response.Status.BAD_REQUEST.getStatusCode(),
                "Unable to process JSON", showDetails ? message : null);
        return Response.status(Response.Status.BAD_REQUEST)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .entity(errorMessage)
                .build();
    }
}