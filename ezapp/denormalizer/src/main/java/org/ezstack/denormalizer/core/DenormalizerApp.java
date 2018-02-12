package org.ezstack.denormalizer.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.apache.samza.operators.functions.SinkFunction;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.ezstack.denormalizer.model.DocumentMessage;
import org.ezstack.denormalizer.serde.JsonSerdeV3;
import org.ezstack.denormalizer.model.Document;
import org.ezstack.ezapp.datastore.api.Query;
import org.ezstack.ezapp.datastore.api.Update;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class DenormalizerApp implements StreamApplication {

    private static final Logger log = LoggerFactory.getLogger(DenormalizerApp.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    Collection<Query> queries = createSampleQuerys();

    public void init(StreamGraph streamGraph, Config config) {

        // TODO: move input stream name into properties
        MessageStream<Update> updates = streamGraph.getInputStream("documents", new JsonSerdeV3<>(Update.class));

        MessageStream<Document> documents = updates.flatMap(new DocumentResolver());

        IndexToESFunction indexToESFunction = new IndexToESFunction();

//        documents.sink(indexToESFunction);

        documents.flatMap(new DocumentMessageMapper(queries))
                .partitionBy(DocumentMessage::getPartitionKey, v -> v, KVSerde.of(new StringSerde(), new JsonSerdeV3<>(DocumentMessage.class)), "partition")
                .map(KV::getValue).flatMap(new JoinFunction()).sink(indexToESFunction);
    }

    private Collection<Query> createSampleQuerys() {
        String jsonObject = "{\n" +
                "  \"searchType\" : [],\n" +
                "  \"table\" : \"teacher\",\n" +
                "  \"join\" : {\n" +
                "    \"table\": \"student\"\n" +
                "  },\n" +
                "  \"joinAttributeName\" : \"student\",\n" +
                "  \"joinAttributes\" : [\n" +
                "    {\n" +
                "      \"outerAttribute\" : \"id\",\n" +
                "      \"innerAttribute\" : \"teacher_id\"\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        String jsonObject1 = "{\n" +
                "  \"searchType\" : [],\n" +
                "  \"table\" : \"student\",\n" +
                "  \"join\" : {\n" +
                "    \"table\": \"teacher\"\n" +
                "  },\n" +
                "  \"joinAttributeName\" : \"teacher\",\n" +
                "  \"joinAttributes\" : [\n" +
                "    {\n" +
                "      \"outerAttribute\" : \"teacher_id\",\n" +
                "      \"innerAttribute\" : \"id\"\n" +
                "    }\n" +
                "  ]\n" +
                "}";
        try {
            return ImmutableSet.of(mapper.readValue(jsonObject, Query.class), mapper.readValue(jsonObject1, Query.class));
        } catch (Exception e) {
            log.error(e.toString());
            return null;
        }
    }

    private class JoinFunction implements FlatMapFunction<DocumentMessage, Document> {

        private KeyValueStore<String, Map<String, Object>> store;

        @Override
        public void init(Config config, TaskContext context) {
            store = (KeyValueStore<String, Map<String, Object>>) context.getStore("join-store");
        }

        private Map<String, Map<String, Document>> getJoinDataStructure(Query query) {
            Query currentQuery = query;
            Integer index = 0;
            Map<String, Map<String, Document>> indexMap = new LinkedHashMap<>();
            do {
                indexMap.put(index.toString(), new HashMap<>());
                index++;
            } while (query.getJoin() != null);

            return indexMap;
        }

        @Override
        public Collection<Document> apply(DocumentMessage message) {
//            if (query.getJoin() == null) {
//                log.info("join is null");
//                return ImmutableSet.of();
//            }
//
//            Document document = message.getDocument();
//            String joinAttName = query.getJoinAttributeName();
//
//            if (query.getTable().equals(document.getTable())) {
//                String dbKey = document.getData().get(query.getJoinAttributes().get(0).getOuterAttribute()).toString();
//
//                if (dbKey == null) return ImmutableSet.of(document);
//
//                Map<String, Object> storedIndex = store.get(dbKey);
//
//                if (storedIndex == null) {
//
//                }
//
//
//
//            }


            return ImmutableSet.of(message.getDocument());

        }
    }

    private class IndexToESFunction implements SinkFunction<Document> {

        @Override
        public void apply(Document document, MessageCollector messageCollector, TaskCoordinator taskCoordinator) {
            messageCollector.send(new OutgoingMessageEnvelope(new SystemStream("elasticsearch", document.getTable() + "/" + document.getTable()),
                    document.getKey(), mapper.convertValue(document, Map.class)));
        }
    }

}