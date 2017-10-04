# Samza on Mesos
This project is designed to enable Samza jobs to run natively on Mesos.
**currently under development**

## Configuration Values


| Property                           | Required? | Default value             | Description                               |
|------------------------------------|-----------|---------------------------|-------------------------------------------|
| mesos.master.connect               | yes       |                           | Mesos master URL                          |
| mesos.package.path                 | yes*      |                           | Job package URI (file, http, hdfs)        |
| mesos.docker.image                 | yes*      |                           | Docker image (registry/my-jobs:latest)    |
| mesos.docker.entrypoint.arguments  |           |                           | Arguments for Docker image ENTRYPOINT     |
| mesos.executor.count               |           | 1                         | Number of Samza containers to run job in  |
| mesos.executor.memory.mb           |           | 1024                      | Mesos task memory constraint              |
| mesos.executor.cpu.cores           |           | 1                         | Mesos task CPU cores constraint           |
| mesos.executor.disk.mb             |           | 2048                      | Mesos task disk constraint                |
| mesos.executor.attributes.*        |           |                           | Slave attributes reqs (regex expressions) |
| mesos.scheduler.user               |           |                           | System user for starting executors        |
| mesos.scheduler.role               |           |                           | Mesos role to use for this scheduler      |
| mesos.scheduler.jmx.enabled        |           | true                      | Mesos role to use for this scheduler      |
| mesos.scheduler.failover.timeout   |           | Long.MaxValue             | Framework failover timeout                |

** either `mesos.package.path` or `mesos.docker.image` is required.

## Credit
This project used many other projects for inspiration here are a few of them:
- https://github.com/InnovaCo/samza-mesos
- https://github.com/apache/samza/tree/master/samza-azure
- https://github.com/apache/samza/tree/master/samza-yarn/src