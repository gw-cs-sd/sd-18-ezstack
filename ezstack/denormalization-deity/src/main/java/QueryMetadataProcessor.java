import com.codahale.metrics.Histogram;
import org.apache.samza.operators.functions.MapFunction;
import org.ezstack.ezapp.querybus.api.QueryMetadata;

public class QueryMetadataProcessor implements MapFunction<QueryMetadata, QueryMetadata> {

    private final DeityMetricRegistry _metrics;
    private final DeityMetricRegistry.MetricSupplier<Histogram> _histogramSupplier;
    private final QueryCounter _intManager;


    public QueryMetadataProcessor(DeityMetricRegistry metrics, DeityMetricRegistry.MetricSupplier<Histogram> histogramSupplier, QueryCounter intManager) {
        _metrics = metrics;
        _histogramSupplier = histogramSupplier;
        _intManager = intManager;
    }

    /**
     * This function generates the key for each query made to the system, and sends each query's data to be logged in
     * their respective histograms.
     * @param queryMetadata
     * @return
     */
    @Override
    public QueryMetadata apply(QueryMetadata queryMetadata) {
        String queryHash = queryMetadata.getQuery().getMurmur3HashAsString();
        _intManager.increment();

        Histogram histogram = _metrics.histogram(queryHash, _histogramSupplier);
        histogram.update(queryMetadata.getResponseTimeInMs());

        _metrics.updateQueryObject(queryMetadata, _histogramSupplier);

        return queryMetadata;
    }

}
