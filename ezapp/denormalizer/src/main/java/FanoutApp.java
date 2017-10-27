import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class FanoutApp implements StreamApplication {

    private static final Logger log = LoggerFactory.getLogger(FanoutApp.class);

    public void init(StreamGraph streamGraph, Config config) {
        MessageStream<Map<String, Object>> updates = streamGraph.<String, Map<String, Object>, Map<String, Object>>getInputStream("test", (k, v) -> v);

        OutputStream<String, Map<String, Object>, Map<String, Object>> outputStream = streamGraph
                .getOutputStream("test_output", msg -> "placeholder_key", msg -> msg);

        System.out.println("APPLICATION STARTING!!!!!");

        updates
                .map( msg -> {
                    log.info(msg.toString());
                    return msg;
                })
        .sendTo(outputStream);
    }
}

