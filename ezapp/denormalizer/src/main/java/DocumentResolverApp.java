import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.task.TaskContext;
import org.ezstack.ezapp.datastore.api.Document;
import org.ezstack.ezapp.datastore.api.Update;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class DocumentResolverApp implements StreamApplication {

    private static final Logger log = LoggerFactory.getLogger(DocumentResolverApp.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    public void init(StreamGraph streamGraph, Config config) {
        MessageStream<Map<String, Object>> updates = streamGraph.<String, Map<String, Object>, Map<String, Object>>getInputStream("test", (k, v) -> v);

        OutputStream<String, Map<String, Object>, Map<String, Object>> outputStream = streamGraph
                .getOutputStream("test_output", msg -> "placeholder_key", msg -> msg);

        updates
                .map( msg -> mapper.convertValue(msg, Update.class))
                .map(new ResolveFunction());
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

}