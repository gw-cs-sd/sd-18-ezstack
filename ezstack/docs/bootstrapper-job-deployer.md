# Bootstrap Job Deployer

The bootstrap job deployer is a simple light weight Flask web server
written in python that can be used to deploy new Samza jobs.

The web server has the following parameters.

| Parameter | Required | Description |
| --- | --- | --- |
| --port |  | specifies the port of the flask web server. Default is port `8000` |
| -p, --path | Yes | specifies the boostrapper Samza job properties path. |
| -rp, --rpath | Yes | specifies the bootstrapper Samza run-app.sh path. |

### Example Samza Bootstrapper Properties

The boostrapper properties that are passed to the
`bootstrap-deployer.py` are expected to be in **`yaml`** format.

A sample job depolyer properties can be found [here](../bootstrapper-job-deployer/example.yaml).
Also for convenience the example is provided below.

```yaml
# Application / Job
app.class: org.ezstack.denormalizer.bootstrapper.BootstrapperApp
app.runner.class: org.apache.samza.runtime.RemoteApplicationRunner

job.factory.class: org.apache.samza.job.yarn.YarnJobFactory
job.name: bootstrapper-app
job.default.system: kafka
job.systemstreampartition.grouper.factory: org.apache.samza.container.grouper.stream.GroupByPartitionFactory
job.container.count: 1

# YARN
yarn.package.path: "file:///opt/ezstack/denormalizer/target/ezstack-denormalizer-0.1-SNAPSHOT-dist.tar.gz"

# Broadcast inputs
task.broadcast.inputs: kafka.shutdown-messages#0

# Serializers
serializers.registry.json.class: org.apache.samza.serializers.JsonSerdeFactory
serializers.registry.string.class: org.apache.samza.serializers.StringSerdeFactory
serializers.registry.document.class: org.ezstack.denormalizer.serde.DocumentSerdeFactory
serializers.registry.join-query-index.class: org.ezstack.denormalizer.serde.JoinQueryIndexSerdeFactory
serializers.registry.document-message.class: org.ezstack.denormalizer.serde.DocumentMessageSerdeFactory

# Kafka System
systems.kafka.samza.factory: org.apache.samza.system.kafka.KafkaSystemFactory
systems.kafka.consumer.zookeeper.connect: localhost:2181
systems.kafka.producer.bootstrap.servers: localhost:9092
systems.kafka.default.stream.replication.factor: 1
systems.kafka.default.stream.samza.msg.serde: json
systems.kafka.default.stream.samza.key.serde: string
systems.kafka.default.stream.samza.offset.default: oldest

# Checkpointing
task.checkpoint.system: kafka
task.checkpoint.replication.factor: 1
task.checkpoint.factory: org.apache.samza.checkpoint.kafka.KafkaCheckpointManagerFactory

# Key-value storage
stores.bootstrapper-document-resolver.factory: org.apache.samza.storage.kv.RocksDbKeyValueStorageEngineFactory
stores.bootstrapper-document-resolver.changelog: kafka.boostrapper-document-resolver-changelog
stores.bootstrapper-document-resolver.changelog.replication.factor: 1
stores.bootstrapper-document-resolver.changelog.cleanup.policy: compact
stores.bootstrapper-document-resolver.changelog.min.compaction.lag.ms: 3600000
stores.bootstrapper-document-resolver.key.serde: string
stores.bootstrapper-document-resolver.msg.serde: document

# Physical Name For Input Stream
streams.documents.samza.system: kafka
streams.documents.samza.physical.name: document-resolver-changelog
streams.documents.samza.bootstrap: true

streams.shutdown-messages.samza.system: kafka
streams.shutdown-messages.samza.physical.name: shutdown-messages

# Physical Name For Output Stream
streams.bootstrapped-document-messages.system: kafka
streams.bootstrapped-document-messages.physical.name: bootstrapped-document-messages

# Job-Specific Configuration
bootstrapper.zkHosts: localhost:2181

# Metrics
metrics.reporters: "snapshot,jmx"
metrics.reporter.snapshot.class: org.apache.samza.metrics.reporter.MetricsSnapshotReporterFactory
metrics.reporter.snapshot.stream: kafka.metrics
metrics.reporter.jmx.class: org.apache.samza.metrics.reporter.JmxReporterFactory
```

### Running the Web Server

Assuming you are inside the `bootstrapper-job-deployer` directory.

```bash
$ pip3 install -r requirements.txt
$ python3 bootstrap-deployer.py -p /path/to/bootstrapper-props.yaml -rp /path/to/run-app.sh
```