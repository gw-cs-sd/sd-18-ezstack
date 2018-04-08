package org.ezstack.denormalizer.bootstrapper;

import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.TaskCoordinator;
import org.ezstack.denormalizer.core.DocumentMessageMapper;
import org.ezstack.denormalizer.model.DocumentChangePair;
import org.ezstack.denormalizer.model.DocumentMessage;
import org.ezstack.denormalizer.model.TombstoningPolicy;
import org.ezstack.denormalizer.serde.JsonSerdeV3;
import org.ezstack.ezapp.datastore.api.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.ezstack.denormalizer.bootstrapper.CuratorRuleRetriever.getRulesForJobID;

public class BootstrapperApp implements StreamApplication {

    private static final Logger log = LoggerFactory.getLogger(BootstrapperApp.class);

    public void init(StreamGraph streamGraph, Config config) {

        // TODO: move input stream name into properties
        MessageStream<Document> documents = streamGraph.getInputStream("documents",
                new JsonSerdeV3<>(Document.class));

        streamGraph.getInputStream("shutdown-message",
                new JsonSerdeV3<>(ShutdownMessage.class))
        .sink((a, b, taskCoordinator) -> taskCoordinator.shutdown(TaskCoordinator.RequestScope.CURRENT_TASK));

        MessageStream<DocumentChangePair> changePairs = documents.map(
                new BootstrappingDocumentResolver("bootstrapper-document-resolver"));

        String zkHosts = "localhost:2181";
        String jobId = config.get("job.id");


        changePairs.flatMap(new DocumentMessageMapper(new BootstrapperRuleIndexer(getRulesForJobID(zkHosts, jobId)),TombstoningPolicy.NO_TOMBSTONING))
                .sink((msg, collector, coordinator) -> {
                    collector.send(new OutgoingMessageEnvelope(
                            new SystemStream("kafka", "bootstrapped-document-messages"),
                            msg.getPartitionKey(), msg));
                });
//                .partitionBy(DocumentMessage::getPartitionKey, v -> v, KVSerde.of(new StringSerde(), new JsonSerdeV3<>(DocumentMessage.class)), "partition")
//                .sendTo(streamGraph.getOutputStream("bootstrapped-document-messages", KV.of(new StringSerde(), new JsonSerdeV3<>(DocumentMessage.class))));
    }
}