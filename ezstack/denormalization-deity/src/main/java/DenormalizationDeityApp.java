import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.UniformReservoir;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;
import org.coursera.metrics.datadog.DatadogReporter;
import org.coursera.metrics.datadog.DatadogReporter.Expansion;
import org.ezstack.ezapp.querybus.api.QueryMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.coursera.metrics.datadog.transport.HttpTransport;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class DenormalizationDeityApp implements StreamApplication {
    private static final Logger LOG = LoggerFactory.getLogger(DenormalizationDeityApp.class);

    private final MetricRegistry _metrics;
    private final MetricRegistry.MetricSupplier<Histogram> _histogramSupplier;
    private Map<String, QueryObject> _priorityObjects;
    private final Date _timestamp = new Date();

    private long _globalStamp;
    private long _adjustmentPeriodMS = 0; // This is set up in the config file, this is the amount of time between updates, in milliseconds.

    public DenormalizationDeityApp() {
        _metrics = new MetricRegistry();
        _histogramSupplier = () -> new Histogram(new UniformReservoir());
        _priorityObjects = new HashMap<>();
        _globalStamp = _timestamp.getTime();
    }

    @Override
    public void init(StreamGraph streamGraph, Config config) {
        DeityConfig deityConfig = new DeityConfig(config);
        _adjustmentPeriodMS = deityConfig.getAdjustmentPeriod();

        MessageStream<QueryMetadata> queryStream = streamGraph.getInputStream("queries", new JsonSerdeV3<>(QueryMetadata.class));
        queryStream.map(this::processQueryMetadata);

        MessageStream<KV<String, QueryMetadata>> partionedQueryMetadata =
                queryStream.partitionBy(queryMetadata -> queryMetadata.hash(),
                        queryMetadata -> queryMetadata,
                        KVSerde.of(new StringSerde(), new JsonSerdeV3<>(QueryMetadata.class)),
                        "partition-query-metadata");
        HttpTransport transport = new HttpTransport.Builder().withApiKey(deityConfig.getDatadogKey()).build();
        DatadogReporter reporter = DatadogReporter.forRegistry(_metrics).withTransport(transport).withExpansions(Expansion.ALL).build();

        reporter.start(10, TimeUnit.SECONDS);
    }

    private QueryMetadata processQueryMetadata(QueryMetadata queryMetadata) {
        String strippedQuery = queryMetadata.toString();

        Histogram histogram = _metrics.histogram(strippedQuery, _histogramSupplier);
        histogram.update(queryMetadata.getResponseTimeInMs());

        updateQueryObject(strippedQuery);

        long tempstamp = _timestamp.getTime();
        if (tempstamp - _adjustmentPeriodMS >= _globalStamp) {
            _globalStamp = tempstamp;
            rules();
        }

        return queryMetadata;
    }

    private void updateQueryObject(String strippedQuery) { //Priority is represented as the Median, skewed by the mean absolute deviation from the median
        Histogram histogram = _metrics.histogram(strippedQuery, _histogramSupplier);
        Snapshot snap = histogram.getSnapshot();

        long mean = (long) snap.getMean();
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
        }
        else {
            queryObject = new QueryObject(strippedQuery, priority, stamp);
            _priorityObjects.put(strippedQuery, queryObject);
        }
    }

    private void rules() {
        long threshold = startRuleCreation();
        addRules(threshold);
    }

    private long startRuleCreation() {
        Histogram histogram = _metrics.histogram("baseline", _histogramSupplier);
        Snapshot snap = histogram.getSnapshot();

        for(Map.Entry<String, QueryObject> entry : _priorityObjects.entrySet()) {
            QueryObject value = entry.getValue();
            histogram.update(value.getPriority());
        }

        return (long)snap.get75thPercentile();
    }

    private void addRules(long threshold) {
        for(Map.Entry<String, QueryObject> entry : _priorityObjects.entrySet()) {
            String key = entry.getKey();
            QueryObject value = entry.getValue();

            if (value.getPriority() >= threshold) {
                //add the rule
                LOG.info("We are adding the rule for " + key);
            }
            else {
                //if the rule exists, remove the rule
                LOG.info("We are removing the rule for " + key);
            }
        }
    }
}