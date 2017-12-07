package org.ezstack.ezapp.datastore;

import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.ezstack.ezapp.datastore.api.DataReader;
import org.ezstack.ezapp.datastore.db.elasticsearch.ElasticSearchConfiguration;
import org.ezstack.ezapp.datastore.db.elasticsearch.ElasticSearchDataReader;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

public class ReaderModule extends PrivateModule {
    private final ElasticSearchConfiguration _elasticSearchConfiguration;

    public ReaderModule(ElasticSearchConfiguration elasticSearchConfiguration) {
        _elasticSearchConfiguration = elasticSearchConfiguration;
    }

    @Override
    protected void configure() {
        bind(DataReader.class).to(ElasticSearchDataReader.class);

        /*
        If the RestHighLevelClient api comes out of beta and we have time we should use it instead.
        https://www.elastic.co/guide/en/elasticsearch/client/java-rest/master/java-rest-high-getting-started-initialization.html
         */
        bind(Client.class).to(PreBuiltTransportClient.class);
        expose(DataReader.class);
    }

    @Provides
    @Singleton
    public PreBuiltTransportClient getBuiltTransportClient() {
        Settings settings = Settings.builder()
                .put("cluster.name", _elasticSearchConfiguration.getClusterName())
                .put("client.transport.sniff", true)
                .build();
        PreBuiltTransportClient client = new PreBuiltTransportClient(settings);

        List<ElasticSearchConfiguration.TransportAddressConfig> transportAddresses = _elasticSearchConfiguration.getTransportAddresses();
        if (transportAddresses == null) {
            return client;
        }

        for (ElasticSearchConfiguration.TransportAddressConfig node: transportAddresses) {
            try {
                client.addTransportAddress(new TransportAddress(InetAddress.getByName(node.getAddress()), node.getPort()));
            } catch (UnknownHostException e) {
                // Maybe log it?
            }
        }
        return client;
    }
}
