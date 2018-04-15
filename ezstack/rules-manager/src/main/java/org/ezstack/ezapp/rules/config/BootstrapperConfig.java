package org.ezstack.ezapp.rules.config;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import java.util.Properties;

public class BootstrapperConfig extends Properties {

    @Inject
    public BootstrapperConfig(@Named("replicationFactor") int replicationFactor,
                              @Named("bootstrapTopicName") String bootstrapTopicName,
                              @Named("shutdownTopicName") String shutdownTopicName,
                              @Named("kafkaBootstrapServers") String kafkaBootstrapServers,
                              @Named("zookeeperHosts") String zookeeperHosts,
                              @Named("bootstrapperContainerCount") int bootstrapperContainerCount,
                              @Named("bootstrapperPackagePath") String bootstrapperPackagePath) {
        super();
        this.setProperty("app.class", "org.ezstack.denormalizer.bootstrapper.BootstrapperApp");
        this.setProperty("app.runner.class", "org.apache.samza.runtime.RemoteApplicationRunner");
        this.setProperty("job.factory.class", "org.apache.samza.job.yarn.YarnJobFactory");
        this.setProperty("job.name", "bootstrapper-app");
        this.setProperty("job.default.system", "kafka");
        this.setProperty("job.systemstreampartition.grouper.factory", "org.apache.samza.container.grouper.stream.GroupByPartitionFactory");
        this.setProperty("job.container.count", Integer.toString(bootstrapperContainerCount));
        this.setProperty("yarn.package.path", bootstrapperPackagePath);
        this.setProperty("serializers.registry.json.class", "org.apache.samza.serializers.JsonSerdeFactory");
        this.setProperty("serializers.registry.string.class", "org.apache.samza.serializers.StringSerdeFactory");
        this.setProperty("serializers.registry.document.class", "org.ezstack.denormalizer.serde.DocumentSerdeFactory");
        this.setProperty("serializers.registry.join-query-index.class", "org.ezstack.denormalizer.serde.JoinQueryIndexSerdeFactory");
        this.setProperty("serializers.registry.document-message.class", "org.ezstack.denormalizer.serde.DocumentMessageSerdeFactory");
        this.setProperty("systems.kafka.samza.factory", "org.apache.samza.system.kafka.KafkaSystemFactory");
        this.setProperty("systems.kafka.consumer.zookeeper.connect", zookeeperHosts);
        this.setProperty("systems.kafka.producer.bootstrap.servers", kafkaBootstrapServers);
        this.setProperty("systems.kafka.default.stream.replication.factor", Integer.toString(replicationFactor));
        this.setProperty("systems.kafka.default.stream.samza.msg.serde", "json");
        this.setProperty("systems.kafka.default.stream.samza.key.serde", "string");
        this.setProperty("systems.kafka.default.stream.samza.offset.default", "oldest");
        this.setProperty("task.checkpoint.system", "kafka");
        this.setProperty("task.checkpoint.replication.factor", Integer.toString(replicationFactor));
        this.setProperty("task.checkpoint.factory", "org.apache.samza.checkpoint.kafka.KafkaCheckpointManagerFactory");
        this.setProperty("stores.bootstrapper-document-resolver.factory", "org.apache.samza.storage.kv.RocksDbKeyValueStorageEngineFactory");
        this.setProperty("stores.bootstrapper-document-resolver.changelog", "kafka.bootstrapper-document-resolver-changelog");
        this.setProperty("stores.bootstrapper-document-resolver.changelog.replication.factor", Integer.toString(replicationFactor));
        this.setProperty("stores.bootstrapper-document-resolver.changelog.cleanup.policy", "compact");
        this.setProperty("stores.bootstrapper-document-resolver.changelog.min.compaction.lag.ms", "3600000");
        this.setProperty("stores.bootstrapper-document-resolver.key.serde", "string");
        this.setProperty("stores.bootstrapper-document-resolver.msg.serde", "document");
        this.setProperty("streams.documents.samza.system", "kafka");
        this.setProperty("streams.documents.samza.physical.name", "document-resolver-changelog");
        this.setProperty("streams.documents.samza.bootstrap", "true");
        this.setProperty("streams.shutdown-messages.samza.system", "kafka");
        this.setProperty("streams.shutdown-messages.samza.physical.name", shutdownTopicName);
        this.setProperty("streams.bootstrapped-document-messages.system", "kafka");
        this.setProperty("streams.bootstrapped-document-messages.physical.name", bootstrapTopicName);
        this.setProperty("bootstrapper.zkHosts", zookeeperHosts);
        this.setProperty("metrics.reporters", "snapshot,jmx");
        this.setProperty("metrics.reporter.snapshot.class", "org.apache.samza.metrics.reporter.MetricsSnapshotReporterFactory");
        this.setProperty("metrics.reporter.snapshot.stream", "kafka.metrics");
        this.setProperty("metrics.reporter.jmx.class", "org.apache.samza.metrics.reporter.JmxReporterFactory");
    }

    private BootstrapperConfig(Properties defaults) {
        super(defaults);
    }

    public BootstrapperConfig forJobId(String jobId) {
        BootstrapperConfig config = new BootstrapperConfig(this);
        config.setProperty("job.id", jobId);
        return config;
    }
}
