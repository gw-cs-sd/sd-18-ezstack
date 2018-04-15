package org.ezstack.deity;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.UniformReservoir;
import com.google.common.base.Suppliers;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;
import org.coursera.metrics.datadog.DatadogReporter;
import org.coursera.metrics.datadog.transport.HttpTransport;
import org.ezstack.ezapp.client.EZappClientFactory;
import org.ezstack.ezapp.datastore.api.Rule;
import org.ezstack.ezapp.datastore.api.RulesManager;
import org.ezstack.ezapp.querybus.api.QueryMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class DenormalizationDeityApp implements StreamApplication {
    private static final Logger LOG = LoggerFactory.getLogger(DenormalizationDeityApp.class);

    private static final int DATADOG_UPDATE_INTERVAL_SECS = 10;

    private DeityMetricRegistry _queryMetricRegistry;
    private MetricRegistry _metricRegistry;
    private final DeityMetricRegistry.MetricSupplier<Histogram> _histogramSupplier;
    private DeityConfig _config;
    private RulesManager _rulesManager;
    private Supplier<Set<Rule>> _ruleSupplier;
    private final QueryCounter _intManager;
    private RuleCreationService _ruleCreationService;

    public DenormalizationDeityApp() {
        _histogramSupplier = () -> new Histogram(new UniformReservoir());
        _intManager = new QueryCounter();
    }

    /**
     * This is the initialization function for the entire DenormalizationDeity. This is where the queries come in, and
     * @param streamGraph
     * @param config
     */
    @Override
    public void init(StreamGraph streamGraph, Config config) {
        _config = new DeityConfig(config);
        _queryMetricRegistry = new DeityMetricRegistry(_config);
        _metricRegistry = new MetricRegistry();
        _rulesManager = EZappClientFactory.newRulesManager(_config.getUriAddress());
        _ruleSupplier = Suppliers.memoizeWithExpiration(_rulesManager::getRules, _config.getCachePeriod(), TimeUnit.SECONDS);
        _ruleCreationService = new RuleCreationService(_config, _queryMetricRegistry, _histogramSupplier, _rulesManager, _ruleSupplier, _intManager);

        MessageStream<QueryMetadata> queryStream = streamGraph.getInputStream("queries", new JsonSerdeV3<>(QueryMetadata.class));

        MessageStream<KV<String, QueryMetadata>> partionedQueryMetadata =
                queryStream.partitionBy(queryMetadata -> queryMetadata.getQuery().getMurmur3HashAsString(),
                        queryMetadata -> queryMetadata,
                        KVSerde.of(new StringSerde(), new JsonSerdeV3<>(QueryMetadata.class)),
                        "partition-query-metadata");

        queryStream.map(new QueryMetadataProcessor(_queryMetricRegistry, _histogramSupplier, _intManager, _rulesManager, _ruleSupplier, _config, _metricRegistry))
                .sink(new RuleCreationServiceSamzaWrapper(_ruleCreationService));

        if (_config.getDatadogKey() != null) {
            HttpTransport transport = new HttpTransport.Builder().withApiKey(_config.getDatadogKey()).build();
            DatadogReporter reporter = DatadogReporter.forRegistry(_metricRegistry).withTransport(transport).withExpansions(DatadogReporter.Expansion.ALL).build();

            reporter.start(DATADOG_UPDATE_INTERVAL_SECS, TimeUnit.SECONDS);
        }
    }
}