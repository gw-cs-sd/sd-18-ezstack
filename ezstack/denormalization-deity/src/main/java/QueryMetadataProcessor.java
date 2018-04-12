import com.codahale.metrics.Histogram;
import org.apache.samza.operators.functions.MapFunction;
import org.ezstack.ezapp.querybus.api.QueryMetadata;

public class QueryMetadataProcessor implements MapFunction<QueryMetadata, QueryMetadata> {

    private final DeityMetricRegistry _metrics;
    private final DeityMetricRegistry.MetricSupplier<Histogram> _histogramSupplier;
    private final AtomicIntManager _intManager;


    public QueryMetadataProcessor(DeityMetricRegistry metrics, DeityMetricRegistry.MetricSupplier<Histogram> histogramSupplier, AtomicIntManager intManager) {
        _metrics = metrics;
        _histogramSupplier = histogramSupplier;
        _intManager = intManager;
    }

    @Override
    public QueryMetadata apply(QueryMetadata queryMetadata) {
        String strippedQuery = queryMetadata.getQuery().getMurmur3HashAsString();
        _intManager.increment();

        Histogram histogram = _metrics.histogram(strippedQuery, _histogramSupplier);
        histogram.update(queryMetadata.getResponseTimeInMs());

        _metrics.updateQueryObject(queryMetadata, _histogramSupplier);

        return queryMetadata;
    }

}
