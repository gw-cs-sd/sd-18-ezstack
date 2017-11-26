package org.ezstack.ezapp.datastore;

import com.google.inject.AbstractModule;
import org.elasticsearch.client.Client;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.ezstack.ezapp.datastore.api.DataReader;
import org.ezstack.ezapp.datastore.db.elasticsearch.ElasticSearchDataReader;

public class ReaderModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(DataReader.class).to(ElasticSearchDataReader.class);

        /*
        If the RestHighLevelClient api comes out of beta and we have time we should use it instead.
        https://www.elastic.co/guide/en/elasticsearch/client/java-rest/master/java-rest-high-getting-started-initialization.html
         */
        bind(Client.class).to(PreBuiltTransportClient.class);
    }
}
