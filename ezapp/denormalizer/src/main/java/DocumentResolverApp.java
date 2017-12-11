import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.functions.FlatMapFunction;
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

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DocumentResolverApp implements StreamApplication {

    private static final Logger log = LoggerFactory.getLogger(DocumentResolverApp.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    public void init(StreamGraph streamGraph, Config config) {

        // TODO: move input stream name into properties
        MessageStream<Update> updates = streamGraph.<String, Map<String, Object>, Update>getInputStream("documents", (k, v) -> mapper.convertValue(v, Update.class));

        updates
                .map(new ResolveFunction())
                .flatMap(new DenormalizeFunction())
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
            String storeKey = update.getDatabase() + update.getTable() + update.getKey();
            Document storedDocument = mapper.convertValue(store.get(storeKey), Document.class);
            if (storedDocument != null) {
//                log.info("Object already existed in store. Need to merge.");
                storedDocument.addUpdate(update);
            }
            else {
//                log.info("Object did not already exist, storing without merge.");
                storedDocument = new Document(update);
            }

            store.put(storeKey, mapper.convertValue(storedDocument, Map.class));

            return storedDocument;
        }
    }

    private class IndexToESFunction implements SinkFunction<Document> {

        @Override
        public void apply(Document document, MessageCollector messageCollector, TaskCoordinator taskCoordinator) {
//            log.info("about to index");
            messageCollector.send(new OutgoingMessageEnvelope(new SystemStream("elasticsearch", document.getDatabase() + "/" + document.getTable()),
                    document.getKey(), document.getData()));
//            log.info("indexed");
        }
    }

    private class DenormalizeFunction implements FlatMapFunction<Document, Document> {

        private KeyValueStore<String, Map<String, Object>> store;

        @Override
        public void init(Config config, TaskContext context) {
            store = (KeyValueStore<String, Map<String, Object>>) context.getStore("classes");
        }

        @Override
        public Collection<Document> apply(Document doc) {
            if (doc.getTable().equals("teacher")) {
                Teacher teacher = mapper.convertValue(doc.getData(), Teacher.class);
                mergeTeacher(teacher, doc.getData());
                Document newDoc = new Document(doc.getDatabase(), "classroom", doc.getKey(), doc.getTimestamp(), doc.getData(), doc.getVersion());
                return ImmutableSet.of(newDoc);
            } else if (doc.getTable().equals("student")) {
                Student student = mapper.convertValue(doc.getData(), Student.class);
                Map<String, Object> data = mergeStudent(student, doc.getData());
                if (data == null) {
                    return ImmutableSet.of();
                }

                return ImmutableSet.of(new Document(doc.getDatabase(), "classroom", student.getTeacherId(), doc.getTimestamp(), data, doc.getVersion()));

            }

            return ImmutableSet.of();
        }


        private void mergeTeacher(Teacher teacher, Map<String, Object> data) {
            Map<String, Object> doc = store.get(teacher.getId());

            data.put("students", doc != null ? doc.get("students") : new Student[0]);
            store.put(teacher.getId(), data);
        }

        private Map<String, Object> mergeStudent(Student student, Map<String, Object> data) {
            Map<String, Object> doc = store.get(student.getTeacherId());

            if (doc == null) {
                doc = new HashMap<>();
                doc.put("students", ImmutableSet.of(data));
                store.put(student.getTeacherId(), doc);
                return null;
            }

//            log.info(doc.toString());
//            try {
//                log.info(mapper.writeValueAsString(doc) + "\n\n");
//            } catch (Exception e) {
//                log.info("json conversion failed");
//            }
//            log.info("HELLO\n\n\n");
//
//            log.info(data.toString());

            List<Student> students = mapper.convertValue(doc.get("students"), new TypeReference<List<Student>>(){});
            boolean studentAdded = false;
            for (int i = 0; i < students.size(); i++) {
                if (students.get(i).getId().equals(student.getId())) {
                    students.set(i, student);
                    studentAdded = true;
                    break;
                }
            }

            if (!studentAdded) {
                students.add(student);
            }

            doc.put("students", mapper.convertValue(students, new TypeReference<List<Map<String, Object>>>(){}));

//            log.info("BEFORE STORE\n\n\n\n" + doc);

            store.put(student.getTeacherId(), doc);


            return doc.get("id") != null ? doc : null;
        }
    }

}