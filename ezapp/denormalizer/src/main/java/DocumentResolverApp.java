import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.operators.functions.SinkFunction;
import org.apache.samza.serializers.JsonSerdeV2;
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

import java.util.Map;

public class DocumentResolverApp implements StreamApplication {

    private static final Logger log = LoggerFactory.getLogger(DocumentResolverApp.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    public void init(StreamGraph streamGraph, Config config) {

        // TODO: move input stream name into properties
        MessageStream<Update> updates = streamGraph.<Update>getInputStream("documents", new JsonSerdeV3<>(Update.class));

        updates
                .map(new ResolveFunction())
                .sink(new IndexToESFunction());
    }

    private class ResolveFunction implements MapFunction<Update, Document> {

        private KeyValueStore<String, Map<String, Object>> store;

        @Override
        public void init(Config config, TaskContext context) {
            store = (KeyValueStore<String, Map<String, Object>>) context.getStore("document-resolver");
        }

        @Override
        public Document apply(Update update) {
            // TODO: replace this storekey with an actual hash function
            String storeKey = update.getTable() + update.getKey();
            Document storedDocument = mapper.convertValue(store.get(storeKey), Document.class);
            if (storedDocument != null) {
                storedDocument.addUpdate(update);
            }
            else {
                storedDocument = new Document(update);
            }

            store.put(storeKey, mapper.convertValue(storedDocument, Map.class));

            return storedDocument;
        }
    }

    private class IndexToESFunction implements SinkFunction<Document> {

        @Override
        public void apply(Document document, MessageCollector messageCollector, TaskCoordinator taskCoordinator) {
            messageCollector.send(new OutgoingMessageEnvelope(new SystemStream("elasticsearch", document.getTable() + "/" + document.getTable()),
                    document.getKey(), document.getData()));
        }
    }

}