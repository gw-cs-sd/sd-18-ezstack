package org.ezstack.denormalizer.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.apache.samza.operators.functions.SinkFunction;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.ezstack.denormalizer.model.DocumentMessage;
import org.ezstack.denormalizer.serde.JsonSerdeV3;
import org.ezstack.denormalizer.model.Document;
import org.ezstack.ezapp.datastore.api.JoinAttribute;
import org.ezstack.ezapp.datastore.api.KeyBuilder;
import org.ezstack.ezapp.datastore.api.Query;
import org.ezstack.ezapp.datastore.api.Update;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.ezstack.ezapp.datastore.api.KeyBuilder.*;

public class DocumentResolverApp implements StreamApplication {

    private static final Logger log = LoggerFactory.getLogger(DocumentResolverApp.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    Collection<Query> queries = createSampleQuerys();
    Map<String, List<Query>> queryMap = new HashMap<>();

    public void init(StreamGraph streamGraph, Config config) {

        // TODO: move input stream name into properties
        MessageStream<Update> updates = streamGraph.getInputStream("documents", new JsonSerdeV3<>(Update.class));

        MessageStream<Document> documents = updates.flatMap(new ResolveFunction());

        IndexToESFunction indexToESFunction = new IndexToESFunction();

        documents.sink(indexToESFunction);

        documents.flatMap(new FanoutFunction())
                .partitionBy(DocumentMessage::getPartitionKey, DocumentMessage::getDocument, "partition").flatMap(new JoinFunction()).sink(indexToESFunction);
    }

    private void buildQueryMap() {
        for (Query query : queries) {
            Query outerQuery = query;
            while (query != null) {
                String tableKey = query.getTable();
                List<Query> queriesForTable = MoreObjects.firstNonNull(queryMap.get(tableKey), new LinkedList<>());
                queriesForTable.add(outerQuery);
                queryMap.put(tableKey, queriesForTable);
                query = query.getJoin();
            }
        }
    }

    private Collection<Query> createSampleQuerys() {
        String jsonObject = "{\n" +
                "  \"searchType\" : [],\n" +
                "  \"table\" : \"teachers\",\n" +
                "  \"join\" : {\n" +
                "    \"table\": \"students\"\n" +
                "  },\n" +
                "  \"joinAttributeName\" : \"students\",\n" +
                "  \"joinAttributes\" : [\n" +
                "    {\n" +
                "      \"outerAttribute\" : \"id\",\n" +
                "      \"innerAttribute\" : \"teacher_id\"\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        String jsonObject1 = "{\n" +
                "  \"searchType\" : [],\n" +
                "  \"table\" : \"students\",\n" +
                "  \"join\" : {\n" +
                "    \"table\": \"teachers\"\n" +
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

    private class FanoutFunction implements FlatMapFunction<Document, DocumentMessage> {

        // This is a simple pass through implementation for the time being
        @Override
        public Collection<DocumentMessage> apply(Document document) {
            String table = document.getTable();
            Collection<Query> applicableQueries = MoreObjects.firstNonNull(queryMap.get(table), ImmutableSet.of());
            Collection<DocumentMessage> messages = new LinkedList<>();

            queryLoop:
            for (Query query : applicableQueries) {

                // apply filters here, and if it is not applicable, then continue

                if (query.getJoin() == null) {
                    messages.add(new DocumentMessage(document, hashKey(query.getTable(), document.getKey())));
                    continue;
                }


                if (query.getTable().equals(document.getTable())) {
                    // TODO: make this hash function avoid collisions
                    String[] valuesForKey = new String[query.getJoinAttributes().size()];
                    Iterator<JoinAttribute> atts = query.getJoinAttributes().iterator();
                    for (int i = 0; i < valuesForKey.length; i++) {
                        valuesForKey[i] = document.getValue(atts.next().getOuterAttribute()).toString();
                        if (valuesForKey[i] == null) continue queryLoop;
                    }
                    messages.add(new DocumentMessage(document, hash(valuesForKey)));
                }

                if (query.getJoin().getTable().equals(document.getTable())) {
                    // TODO: make this hash function avoid collisions
                    String[] valuesForKey = new String[query.getJoinAttributes().size()];
                    Iterator<JoinAttribute> atts = query.getJoinAttributes().iterator();
                    for (int i = 0; i < valuesForKey.length; i++) {
                        valuesForKey[i] = document.getValue(atts.next().getInnerAttribute()).toString();
                        if (valuesForKey[i] == null) continue queryLoop;
                    }
                    messages.add(new DocumentMessage(document, hash(valuesForKey)));
                }
            }

            return messages;

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
            if (query.getJoin() == null) {
                log.info("join is null");
                return ImmutableSet.of();
            }

            Document document = message.getDocument();
            String joinAttName = query.getJoinAttributeName();

            if (query.getTable().equals(document.getTable())) {
                String dbKey = document.getData().get(query.getJoinAttributes().get(0).getOuterAttribute()).toString();

                if (dbKey == null) return ImmutableSet.of(document);

                Map<String, Object> storedIndex = store.get(dbKey);

                if (storedIndex == null) {

                }

            }

        }
    }


    private class ResolveFunction implements FlatMapFunction<Update, Document> {

        private KeyValueStore<String, Map<String, Object>> store;

        @Override
        public void init(Config config, TaskContext context) {
            store = (KeyValueStore<String, Map<String, Object>>) context.getStore("document-resolver");
        }

        @Override
        public Collection<Document> apply(Update update) {
            String storeKey = KeyBuilder.hashKey(update.getTable(), update.getKey());
            Document storedDocument = mapper.convertValue(store.get(storeKey), Document.class);

            if (storedDocument == null) {
                storedDocument = new Document(update);
                store.put(storeKey, mapper.convertValue(storedDocument, Map.class));
                return ImmutableSet.of(storedDocument);
            }

            int versionBeforeUpdate = storedDocument.getVersion();
            storedDocument.addUpdate(update);
            if (storedDocument.getVersion() != versionBeforeUpdate) {
                store.put(storeKey, mapper.convertValue(storedDocument, Map.class));
                return ImmutableSet.of(storedDocument);
            }

            return ImmutableSet.of();
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