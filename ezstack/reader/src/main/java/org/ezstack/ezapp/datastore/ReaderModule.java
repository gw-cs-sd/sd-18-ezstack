package org.ezstack.ezapp.datastore;

import com.google.inject.*;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import org.ezstack.ezapp.common.lifecycle.GuavaManagedService;
import org.ezstack.ezapp.common.lifecycle.LifeCycleRegistry;
import org.ezstack.ezapp.datastore.api.DataReader;
import org.ezstack.ezapp.datastore.api.RulesManager;
import org.ezstack.ezapp.datastore.core.DefaultDataReader;
import org.ezstack.ezapp.datastore.db.elasticsearch.ElasticSearchConfiguration;
import org.ezstack.ezapp.datastore.db.elasticsearch.ElasticSearchDataReaderDAO;
import org.ezstack.ezapp.datastore.db.elasticsearch.TransportAddressConfig;

import java.util.Collections;
import java.util.List;

import static com.google.common.base.MoreObjects.firstNonNull;

public class ReaderModule extends PrivateModule {
    private final ElasticSearchConfiguration _elasticSearchConfiguration;

    public ReaderModule(ElasticSearchConfiguration elasticSearchConfiguration) {
        _elasticSearchConfiguration = elasticSearchConfiguration;
    }

    @Override
    protected void configure() {

        requireBinding(Key.get(RulesManager.class));

        bind(DataReader.class).to(DefaultDataReader.class).asEagerSingleton();
        bind(String.class).annotatedWith(Names.named("clusterName")).toInstance(_elasticSearchConfiguration.getClusterName());
        bind(new TypeLiteral<List<TransportAddressConfig>>(){}).annotatedWith(Names.named("transportAddresses"))
                .toInstance(firstNonNull(_elasticSearchConfiguration.getTransportAddresses(), Collections.emptyList()));
        expose(DataReader.class);
    }

    @Provides
    @Singleton
    ElasticSearchDataReaderDAO provideElasticSearchDataReaderDAO(@Named("clusterName") String clusterName,
                                                                 @Named("transportAddresses") List<TransportAddressConfig> transportAddresses,
                                                                 LifeCycleRegistry lifeCycleRegistry) {
        ElasticSearchDataReaderDAO elasticSearchDataReaderDAO = new ElasticSearchDataReaderDAO(clusterName, transportAddresses);
        lifeCycleRegistry.manage(new GuavaManagedService(elasticSearchDataReaderDAO));
        return elasticSearchDataReaderDAO;
    }
}
