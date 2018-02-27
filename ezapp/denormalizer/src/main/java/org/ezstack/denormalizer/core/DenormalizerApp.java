package org.ezstack.denormalizer.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.task.TaskContext;
import org.ezstack.denormalizer.model.*;
import org.ezstack.denormalizer.serde.JsonSerdeV3;
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

        MessageStream<DocumentChangePair> documents = updates.flatMap(new DocumentResolver("document-resolver"));


        ElasticsearchIndexer elasticsearchIndexer = new ElasticsearchIndexer();

        documents.map(changePair -> new WritableResult(changePair.getNewDocument(),
                changePair.getNewDocument().getTable(), WritableResult.Action.INDEX))
            .sink(elasticsearchIndexer);

        documents.flatMap(new DocumentMessageMapper(queries))
                .partitionBy(DocumentMessage::getPartitionKey, v -> v, KVSerde.of(new StringSerde(), new JsonSerdeV3<>(DocumentMessage.class)), "partition")
                .map(KV::getValue).flatMap(new DocumentJoiner("join-store")).sink(elasticsearchIndexer);
    }

    private Collection<Query> createSampleQuerys() {
        String jsonObject = "{\n" +
                "  \"searchTypes\" : [],\n" +
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
                "  \"searchTypes\" : [],\n" +
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

}