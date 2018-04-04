package org.ezstack.denormalizer.core;

import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;
import org.ezstack.denormalizer.model.*;
import org.ezstack.denormalizer.serde.JsonSerdeV3;
import org.ezstack.ezapp.datastore.api.Update;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class DenormalizerApp implements StreamApplication {

    private static final Logger log = LoggerFactory.getLogger(DenormalizerApp.class);

    public void init(StreamGraph streamGraph, Config config) {

        // TODO: move input stream name into properties
        MessageStream<Update> updates = streamGraph.getInputStream("documents", new JsonSerdeV3<>(Update.class));

        MessageStream<DocumentChangePair> documents = updates.flatMap(new DocumentResolver("document-resolver"));


        ElasticsearchIndexer elasticsearchIndexer = new ElasticsearchIndexer("elasticsearch");

        documents.map(changePair -> new WritableResult(changePair.getNewDocument(),
                changePair.getNewDocument().getTable(), WritableResult.Action.INDEX))
                .sink(elasticsearchIndexer);

        documents.flatMap(new DocumentMessageMapper("localhost:2181", "/rules"))
                .partitionBy(DocumentMessage::getPartitionKey, v -> v, KVSerde.of(new StringSerde(), new JsonSerdeV3<>(DocumentMessage.class)), "partition")
                .map(KV::getValue).flatMap(new DocumentJoiner("join-store")).sink(elasticsearchIndexer);
    }
}