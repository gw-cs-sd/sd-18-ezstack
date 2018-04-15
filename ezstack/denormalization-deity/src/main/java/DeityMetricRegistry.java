import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import org.ezstack.ezapp.datastore.api.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class DeityMetricRegistry extends MetricRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(DeityMetricRegistry.class);

    private static final int MAX_QUERY_COUNT = 1024;

    private AtomicLong _histogramCounter;
    private Date _stamper = new Date();
    private Map<String, QueryObject> _priorityObjects;
    private DeityConfig _config;

    public DeityMetricRegistry(DeityConfig config) {
        super();
        _config = config;
        _priorityObjects = new ConcurrentHashMap<>();
        _histogramCounter = new AtomicLong(0);
    }

    /**
     * Priority is represented as the median, skewed by the mean absolute deviation from the median. The purpose of
     * the mean absolute deviation is to provide a score that weighs all parts of a query response time, so that some
     * queries that are skewed heavily towards a higher or lower percentage can be evaluated accurately.
     * @param rule
     * @param metricSupplier
     */
    public void updateQueryObject(Rule rule, MetricSupplier metricSupplier) {
        String queryHash = rule.getQuery().getMurmur3HashAsString();
        Histogram histogram = this.histogram(queryHash, metricSupplier);
        Snapshot snap = histogram.getSnapshot();

        long mean = (long) snap.getMean();

        // This check and fix is to prevent an edge case where it was possible to have a dividebyzero exception.
        if (mean == 0) {
            mean = 1;
        }

        long median = (long) snap.getMedian();
        long[] values = snap.getValues();
        long meanAbsoluteDeviation = 0;

        for (int i = 0; i < values.length; i++) {
            meanAbsoluteDeviation += Math.abs(values[i]-median);
        }
        meanAbsoluteDeviation = meanAbsoluteDeviation/mean;

        long priority;
        if (median <= mean) {
            priority = median - meanAbsoluteDeviation;
        }
        else {
            priority = median + meanAbsoluteDeviation;
        }

        long stamp = _stamper.getTime();
        QueryObject queryObject;

        if (_priorityObjects.containsKey(queryHash)) {
            queryObject = _priorityObjects.get(queryHash);
            queryObject.setRecentTimestamp(stamp);
            queryObject.setPriority(priority);
        }
        else {
            queryObject = new QueryObject(priority, stamp, rule);
            if (_histogramCounter.incrementAndGet() == _config.getMaxHistogramCount()) {
                prune(metricSupplier);
            }
            _priorityObjects.put(queryHash, queryObject);
        }
    }

    public Map<String, QueryObject> getQueryObjects() {
        return _priorityObjects;
    }

    /**
     * This function is used to prevent overflows of data, when too many unique queries are being in the system.
     * The function first looks for empty (or near-empty) queries, and if it still needs to remove additional queries,
     * it looks for the oldest queries to remove until it has opened up a sufficient space for new queries.
     * @param metricSupplier
     */
    public void prune(MetricSupplier metricSupplier) {
        List<QueryObject> sortedList = new LinkedList();

        Iterator<Map.Entry<String, QueryObject>> iter = _priorityObjects.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, QueryObject> entry = iter.next();

            Histogram histogram = this.histogram(entry.getKey(), metricSupplier);
            Snapshot snap = histogram.getSnapshot();

            if (snap.size() < MAX_QUERY_COUNT) {
                this.remove(entry.getKey());
                iter.remove();
                _histogramCounter.decrementAndGet();
            }

            sortedList.add(entry.getValue());
        }

        sortedList.sort((x, y) -> (int) (x.getRecentTimestamp() - y.getRecentTimestamp()));

        // We need to make sure that when we prune, we're opening up at least half of histogram
        while (sortedList.size() > _config.getMaxHistogramCount() / 2) {
            QueryObject object = sortedList.remove(0);

            this.remove(object.getRule().getQuery().getMurmur3HashAsString());
            _priorityObjects.remove(object);

            _histogramCounter.decrementAndGet();
        }
    }

}
