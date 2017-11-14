import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.StreamGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class DenormalizationDeityApp implements StreamApplication {

    private static final Logger log = LoggerFactory.getLogger(DenormalizationDeityApp.class);

    @Override
    public void init(StreamGraph streamGraph, Config config) {
        MessageStream<Map<String, Object>> queries = streamGraph.<String, Map<String, Object>, Map<String, Object>>getInputStream("queries", (key, msg) -> msg);

        queries.map(this::printMessage);
    }

    private Map<String, Object> printMessage(Map<String, Object> msg) {
        log.info(msg.toString());
        return msg;
    }

}
