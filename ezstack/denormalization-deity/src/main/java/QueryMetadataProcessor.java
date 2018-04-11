import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import org.apache.samza.operators.functions.MapFunction;
import org.ezstack.ezapp.querybus.api.QueryMetadata;

import java.util.Date;
import java.util.Map;

public class QueryMetadataProcessor implements MapFunction {

    private final MetricRegistry _metrics;
    private final MetricRegistry.MetricSupplier<Histogram> _histogramSupplier;
    private Map<String, QueryObject> _priorityObjects;
    private final Date _timestamp = new Date();
    private final AtomicIntManager _intManager;


    public QueryMetadataProcessor(MetricRegistry metrics, MetricRegistry.MetricSupplier<Histogram> histogramSupplier, Map<String, QueryObject> priorityObjects, AtomicIntManager intManager) {
        _metrics = metrics;
        _histogramSupplier = histogramSupplier;
        _priorityObjects = priorityObjects;
        _intManager = intManager;
    }

    @Override
    public Object apply(Object message) {
        QueryMetadata queryMetadata = (QueryMetadata) message;
        String strippedQuery = queryMetadata.toString();
        _intManager.increment();

        Histogram histogram = _metrics.histogram(strippedQuery, _histogramSupplier);
        histogram.update(queryMetadata.getResponseTimeInMs());

        updateQueryObject(queryMetadata);

        return queryMetadata;
    }

    private void updateQueryObject(QueryMetadata metadata) { //Priority is represented as the Median, skewed by the mean absolute deviation from the median
        String strippedQuery = metadata.toString();
        Histogram histogram = _metrics.histogram(strippedQuery, _histogramSupplier);
        Snapshot snap = histogram.getSnapshot();

        long mean = (long) snap.getMean();
        if(mean == 0) {
            mean = 1;
        }
        long median = (long) snap.getMedian();

        long[] values = snap.getValues();
        long meanAbsoluteDeviation = 0;

        long priority;

        for (int i = 0; i < values.length; i++) {
            meanAbsoluteDeviation += Math.abs(values[i]-median);
        }
        meanAbsoluteDeviation = meanAbsoluteDeviation/mean;

        if(median <= mean) {
            priority = median - meanAbsoluteDeviation;
        }
        else {
            priority = median + meanAbsoluteDeviation;
        }

        long stamp = _timestamp.getTime();

        QueryObject queryObject;

        if (_priorityObjects.containsKey(strippedQuery)) {
            queryObject = _priorityObjects.get(strippedQuery);
            queryObject.setRecentTimestamp(stamp);
            queryObject.setPriority(priority);
            queryObject.setQuery(metadata.getQuery());
        }
        else {
            queryObject = new QueryObject(strippedQuery, priority, stamp, metadata.getQuery());
            _priorityObjects.put(strippedQuery, queryObject);
        }
    }

}
