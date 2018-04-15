package org.ezstack.bootstrap.client;

import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.net.URI;

public class BootstrapClient {
    private final Client _client;
    private final URI _uri;

    public BootstrapClient(String uri) {
        _uri = URI.create(uri);
        _client = ClientBuilder.newClient().register(JacksonJsonProvider.class);
    }

    public BootstrapClientResponse startJob(String jobId) throws RuntimeException {
        Response response =  _client
                .target(UriBuilder.fromUri(_uri)
                        .path("/deploy/bootstrap/")
                        .path(jobId))
                .request(MediaType.APPLICATION_JSON)
                .get();

        BootstrapClientResponse ret = response.readEntity(BootstrapClientResponse.class);

        if (response.getStatus() != 201) {
            throw new RuntimeException(ret.toString());
        }

        return ret;
    }
}
