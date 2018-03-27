package org.ezstack.ezapp.datastore.db.elasticsearch;

import com.google.common.util.concurrent.AbstractService;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.ezstack.ezapp.datastore.api.QueryResult;
import org.ezstack.ezapp.datastore.api.RuleExecutor;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Collections;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class ElasticSearchDataReaderDAO extends AbstractService {

    private final String _clusterName;
    private final List<TransportAddressConfig> _transportAddresses;

    private Client _client;

    @Inject
    public ElasticSearchDataReaderDAO(@Named("clusterName") String clusterName,
                                      @Named("transportAddresses") List<TransportAddressConfig> transportAddressConfigs) {
        _clusterName = checkNotNull(clusterName, "clusterName");
        _transportAddresses = checkNotNull(transportAddressConfigs, "transportAddresses");
    }

    @Override
    protected void doStart() {
        Settings settings = Settings.builder()
                .put("cluster.name", _clusterName)
                .put("client.transport.sniff", true)
                .build();
        PreBuiltTransportClient client = new PreBuiltTransportClient(settings);

        for (TransportAddressConfig node: _transportAddresses) {
            try {
                client.addTransportAddress(new TransportAddress(InetAddress.getByName(node.getAddress()), node.getPort()));
            } catch (Exception e) {
                notifyFailed(e);
                throw new RuntimeException(e);
            }
        }
        _client = client;

        notifyStarted();
    }

    @Override
    protected void doStop() {
        try {
            _client.close();
        } catch (Exception e) {
            notifyFailed(e);
            throw e;
        }
        notifyStopped();
    }

    public Map<String, Object> getDocument(String index, String id) {
        try {
            GetResponse response = _client.prepareGet(index, index, id).get();
            return response.getSourceAsMap();
        } catch (IndexNotFoundException e) {
            return Collections.emptyMap();
        }
    }

    public QueryResult getDocuments(long scrollInMillis, int batchSize, RuleExecutor ruleExecutor) {
        return new ElasticQueryParser(scrollInMillis, batchSize, ruleExecutor, _client).getDocuments();
    }
}
