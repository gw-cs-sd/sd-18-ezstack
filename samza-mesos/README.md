# Samza on Mesos
This project is designed to enable Samza jobs to run natively on Mesos.

## Status
Early dev somewhat functional.

## Build
This is a maven project run and tested on java 8. The build will create
a super jar that can be run locally or on marathon (for high avilability).
<br><br>
To build the project traverse to the `samza-mesos` directory and run the following command.
```bash
mvn clean package
```

## Sample Properties File for Samza Mesos
**Note that this is a different convention then usually seen in samza examples**
The following is an example properties file for samza mesos.
```properties
# Job
job.name=wikipedia-feed

# Mesos
mesos.package.path=http://www.mysite.com/hello-samza-dist.tar.gz
mesos.master.connect=zk://www.myzknode.com:2181/mesos
mesos.command=bin/run-app.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/config/wikipedia-feed.properties
mesos.executor.count=4
```

**Inside the wikipedia-feed.properties used in `hello-samza` you need to change the job factory and the domains for kafka and zookeeper**
```properties
job.factory.class=org.apache.samza.job.local.ProcessJobFactory

...

systems.kafka.consumer.zookeeper.connect=www.myzknode.com:2181/
systems.kafka.producer.bootstrap.servers=www.mykafkanode.com:9092
```

## Running it locally
Assuming you are in the same directory as the super jar and custom properties file.
```bash
java -jar samza-mesos-1.0-SNAPSHOT-jar-with-dependencies.jar --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/mesos.properties
```

## Configuration Values

| Property | Required | Default value | Description |
|---|---|---|---|
| mesos.master.connect | yes |  | Mesos master URL |
| mesos.package.path | yes |  | Job package URI (file, http, hdfs) |
| mesos.executor.count | | 1 | Number of Samza containers to run job in |
| mesos.executor.memory.mb | | 1024 | Mesos task memory constraint |
| mesos.executor.cpu.cores |  | 1 | Mesos task CPU cores constraint |
| mesos.executor.disk.mb | | 2048 | Mesos task disk constraint |
| mesos.executor.attributes.* (not implemented) |  | | Slave attributes reqs (regex expressions) |
| mesos.scheduler.user | | | System user for starting executors |
| mesos.scheduler.failover.timeout |  | Integer.MaxValue | Framework failover timeout |
| mesos.command | yes |  | the command that should be run on mesos for this samza job |