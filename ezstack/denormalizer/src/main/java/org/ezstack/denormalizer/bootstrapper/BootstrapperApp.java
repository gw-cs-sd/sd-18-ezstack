package org.ezstack.denormalizer.bootstrapper;

import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.functions.SinkFunction;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.ezstack.denormalizer.core.DocumentMessageMapper;
import org.ezstack.denormalizer.model.DocumentChangePair;
import org.ezstack.denormalizer.model.DocumentMessage;
import org.ezstack.denormalizer.model.TombstoningPolicy;
import org.ezstack.denormalizer.serde.JsonSerdeV3;
import org.ezstack.ezapp.datastore.api.Document;
import org.ezstack.ezapp.datastore.api.ShutdownMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

import static org.ezstack.denormalizer.bootstrapper.CuratorRuleRetriever.getRulesForJobID;

public class BootstrapperApp implements StreamApplication {

    private static final Logger log = LoggerFactory.getLogger(BootstrapperApp.class);

    public void init(StreamGraph streamGraph, Config config) {

        String zkHosts = checkNotNull(config.get("bootstrapper.zkHosts"));
        String jobId = checkNotNull(config.get("job.id"));

        MessageStream<Document> documents = streamGraph.getInputStream("documents",
                new JsonSerdeV3<>(Document.class));

        streamGraph.getInputStream("shutdown-messages",
                new JsonSerdeV3<>(ShutdownMessage.class))
                .sink(new ShutDownSamzaWrapper(zkHosts, "/bootstrapper", jobId));

        MessageStream<DocumentChangePair> changePairs = documents.map(
                new BootstrappingDocumentResolver("bootstrapper-document-resolver"));

        OutputStream<KV<String, DocumentMessage>> documentMessages =
                streamGraph.getOutputStream("bootstrapped-document-messages",
                        KVSerde.of(new StringSerde(), new JsonSerdeV3<>(DocumentMessage.class)));

        changePairs.flatMap(new DocumentMessageMapper(new BootstrapperRuleIndexer(getRulesForJobID(zkHosts, jobId)),TombstoningPolicy.NO_TOMBSTONING))
                .map(msg -> KV.of(msg.getPartitionKey(), msg))
                .sendTo(documentMessages);
    }

    private class ShutDownSamzaWrapper implements SinkFunction<ShutdownMessage> {

        private final String _zkHosts;
        private final String _bootstrapperPath;
        private final String _jobId;

        private String _partition;


        public ShutDownSamzaWrapper(String zkHosts, String bootstrapperPath, String jobId) {
            _zkHosts = checkNotNull(zkHosts);
            _bootstrapperPath = checkNotNull(bootstrapperPath);
            _jobId = checkNotNull(jobId);

        }


        @Override
        public void init(Config config, TaskContext context) {
            _partition = context.getTaskName().getTaskName();
        }

        @Override
        public void apply(ShutdownMessage message, MessageCollector messageCollector, TaskCoordinator taskCoordinator) {
            BootstrapperRuleAcknowledger.acknowledgeBootstrappingComplete(_zkHosts,_bootstrapperPath, _partition, _jobId);
            taskCoordinator.shutdown(TaskCoordinator.RequestScope.CURRENT_TASK);
        }
    }
}