import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.StreamGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class DenormalizationDeityApp implements StreamApplication {

    private static final Logger log = LoggerFactory.getLogger(DenormalizationDeityApp.class);
    private long numQueries = 0;
    private long totalResponseTime = 0;
    private double average = 0;

    @Override
    public void init(StreamGraph streamGraph, Config config) {
        MessageStream<Map<String, Object>> queryStream = streamGraph.<String, Map<String, Object>, Map<String, Object>>getInputStream("queries", (key, msg) -> msg);

        queryStream.map(this::processQuery);
    }

    private Map<String, Object> processQuery(Map<String, Object> query) {
        log.info(query.toString());
        numQueries++;
        totalResponseTime += new Long(((Map) query.get("_data")).get("responseTime").toString()).longValue();
        average = totalResponseTime / numQueries;
        log.info("Average:" + average);
        return query;
    }

}
