import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.operators.functions.SinkFunction;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.ezstack.ezapp.datastore.api.Document;
import org.ezstack.ezapp.datastore.api.Update;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class DocumentResolverApp implements StreamApplication {

    private static final Logger log = LoggerFactory.getLogger(DocumentResolverApp.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    public void testObjectMapper() throws IOException {
        String str = "{\"_table\":\"test\",\"_key\":\"231145\",\"_timestamp\":\"22bef2c0-ce59-11e7-a7d8-8d6dcb64e54f\",\"_data\":{\"author\":\"Bob\",\"title\":\"Hey Paul!\",\"rating\":17},\"_isUpdate\":false}";
        Update update = mapper.readValue(str, Update.class);
        log.info(update.getTable());
        log.info(str);
    }

    public void init(StreamGraph streamGraph, Config config) {
        try {
            testObjectMapper();
        } catch (Exception e) {
            log.error("TEST MAPPER FAILED :(");
        }

        // TODO: move input stream name into properties
        MessageStream<Update> updates = streamGraph.<Update>getInputStream("documents", new JsonSerdeV3<>(Update.class));

//        OutputStream<String, Map<String, Object>, Map<String, Object>> outputStream = streamGraph
//                .getOutputStream("test_output", msg -> "placeholder_key", msg -> msg);

        updates
//                .map( msg -> mapper.convertValue(msg, Update.class))
                .map(new ResolveFunction())
                .sink(new IndexToESFunction());
    }

    private class ResolveFunction implements MapFunction<Update, Map<String, Object>> {

        private KeyValueStore<String, Map<String, Object>> store;

        @Override
        public void init(Config config, TaskContext context) {
            store = (KeyValueStore<String, Map<String, Object>>) context.getStore("document-resolver");
        }

        @Override
        public Map<String, Object> apply(Update update) {
            // TODO: replace this storekey with an actual hash function
            String storeKey = update.getTable() + update.getKey();
            Document storedDocument = mapper.convertValue(store.get(storeKey), Document.class);
            if (storedDocument != null) {
                log.info("Object already existed in store. Need to merge.");
                storedDocument.addUpdate(update);
            }
            else {
                log.info("Object did not already exist, storing without merge.");
                storedDocument = new Document(update);
            }

            store.put(storeKey, mapper.convertValue(storedDocument, Map.class));

            return mapper.convertValue(update, Map.class);
        }
    }

    private class IndexToESFunction implements SinkFunction<Map<String, Object>> {

        @Override
        public void apply(Map<String, Object> objectMap, MessageCollector messageCollector, TaskCoordinator taskCoordinator) {
            log.info("about to index");
            messageCollector.send(new OutgoingMessageEnvelope(new SystemStream("elasticsearch", "test/document"), (String) objectMap.get("_key"), objectMap.get("_data")));
            log.info("indexed");
        }
    }

}