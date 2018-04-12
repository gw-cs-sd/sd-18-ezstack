import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import org.ezstack.ezapp.querybus.api.QueryMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class DeityMetricRegistry extends MetricRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(DeityMetricRegistry.class);

    private final long MAX_HISTOGRAM_COUNT = 5;

    private AtomicLong _histogramCounter;
    private Date _stamper = new Date();
    private Map<String, QueryObject> _priorityObjects;

    public DeityMetricRegistry() {
        super();
        _priorityObjects = new ConcurrentHashMap<>();
        _histogramCounter = new AtomicLong(0);
    }

    // Priority is represented as the median, skewed by the mean absolute deviation from the median
    public void updateQueryObject(QueryMetadata metadata, MetricSupplier _metricSupplier) {
        String strippedQuery = metadata.getQuery().getMurmur3HashAsString();
        Histogram histogram = this.histogram(strippedQuery, _metricSupplier);
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

        long stamp = _stamper.getTime();

        QueryObject queryObject;

        if (_priorityObjects.containsKey(strippedQuery)) {
            queryObject = _priorityObjects.get(strippedQuery);
            queryObject.setRecentTimestamp(stamp);
            queryObject.setPriority(priority);
            queryObject.setQuery(metadata.getQuery());
        }
        else {
            queryObject = new QueryObject(priority, stamp, metadata.getQuery());
            if(_histogramCounter.incrementAndGet() == MAX_HISTOGRAM_COUNT) {
                prune(_metricSupplier);
            }
            _priorityObjects.put(strippedQuery, queryObject);
        }
    }

    public Map<String, QueryObject> getQueryObjects() {
        return _priorityObjects;
    }

    public void prune(MetricSupplier _metricSupplier) {
        List<QueryObject> sortedList = new LinkedList();

        Iterator<Map.Entry<String, QueryObject>> iter = _priorityObjects.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, QueryObject> entry = iter.next();

            Histogram histogram = this.histogram(entry.getKey(), _metricSupplier);
            Snapshot snap = histogram.getSnapshot();

            if(snap.size() < 1024) {
                this.remove(entry.getKey());
                iter.remove();
                _histogramCounter.decrementAndGet();
                LOG.info("reaping");
            }

            sortedList.add(entry.getValue());
        }

        sortedList.sort((x, y) -> (int) (x.getRecentTimestamp() - y.getRecentTimestamp()));

        while(sortedList.size() > MAX_HISTOGRAM_COUNT/2) {
            QueryObject object = sortedList.remove(0);

            this.remove(object.getQuery().getMurmur3HashAsString());
            _priorityObjects.remove(object);

            LOG.info("get reaped");

            _histogramCounter.decrementAndGet();
        }
    }

}
