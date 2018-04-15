import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import org.apache.samza.operators.functions.MapFunction;
import org.ezstack.ezapp.datastore.api.Rule;
import org.ezstack.ezapp.datastore.api.RulesManager;
import org.ezstack.ezapp.querybus.api.QueryMetadata;

import java.util.Set;
import java.util.function.Supplier;

public class QueryMetadataProcessor implements MapFunction<QueryMetadata, QueryMetadata> {

    private final DeityMetricRegistry _queryMetricRegistry;
    private final DeityMetricRegistry.MetricSupplier<Histogram> _histogramSupplier;
    private final QueryCounter _intManager;
    private final RulesManager _rulesManager;
    private final Supplier<Set<Rule>> _ruleSupplier;
    private final DeityConfig _config;

    public QueryMetadataProcessor(DeityMetricRegistry queryMetricRegistry, DeityMetricRegistry.MetricSupplier<Histogram> histogramSupplier, QueryCounter intManager, RulesManager rulesManager, Supplier<Set<Rule>> ruleSupplier, DeityConfig config, MetricRegistry metricRegistry) {
        _queryMetricRegistry = queryMetricRegistry;
        _histogramSupplier = histogramSupplier;
        _intManager = intManager;
        _rulesManager = rulesManager;
        _ruleSupplier = ruleSupplier;
        _config = config;
    }

    /**
     * This function generates the key for each query made to the system, and sends each query's data to be logged in
     * their respective histograms.
     * @param queryMetadata
     * @return
     */
    @Override
    public QueryMetadata apply(QueryMetadata queryMetadata) {
        QueryToRule ruleConverter = new QueryToRule(_rulesManager, _ruleSupplier, _config.getMaxRuleCapacity());
        Rule rule = ruleConverter.convertToRule(queryMetadata.getQuery());

        if (rule == null) {
            return queryMetadata;
        }

        String queryHash = rule.getQuery().getMurmur3HashAsString();
        _intManager.increment();

        Histogram histogram = _queryMetricRegistry.histogram(queryHash, _histogramSupplier);
        histogram.update(queryMetadata.getResponseTimeInMs());

        _queryMetricRegistry.updateQueryObject(rule, _histogramSupplier);

        return queryMetadata;
    }

}
