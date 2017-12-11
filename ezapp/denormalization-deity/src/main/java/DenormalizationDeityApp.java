import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.StreamGraph;
import org.codehaus.jackson.map.ObjectMapper;
import org.coursera.metrics.datadog.DatadogReporter;
import org.coursera.metrics.datadog.DatadogReporter.Expansion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.coursera.metrics.datadog.transport.HttpTransport;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class DenormalizationDeityApp implements StreamApplication {

    private static final Logger log = LoggerFactory.getLogger(DenormalizationDeityApp.class);
    private final MetricRegistry metrics = new MetricRegistry();
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void init(StreamGraph streamGraph, Config config) {
        DeityConfig deityConfig = new DeityConfig(config);
        MessageStream<Query> queryStream = streamGraph.<String, Map<String, Object>, Query>getInputStream("queries", this::convertToQuery);
        queryStream.map(this::processQuery);

        HttpTransport transport = new HttpTransport.Builder().withApiKey(deityConfig.getDatadogKey()).build();
        DatadogReporter reporter = DatadogReporter.forRegistry(metrics).withTransport(transport).withExpansions(Expansion.ALL).build();

        reporter.start(10, TimeUnit.SECONDS);
    }

    private Query convertToQuery(String key, Map<String, Object> msg) {
        return objectMapper.convertValue(msg, Query.class);
    }

    private Query processQuery(Query query) {
        Histogram histogram = metrics.histogram(query.getStrippedQuery());
        histogram.update(query.getResponseTime());
        return query;
    }

}