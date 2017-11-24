/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.ezstack.samza;

import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;

import java.util.Map;

public class MesosConfig extends JobConfig {

    // (Required if not using docker) the job package URI (file, http, hdfs)
    public static final String PACKAGE_PATH = "mesos.package.path";

    // (Required if using docker) the docker image
    public static final String DOCKER_IMAGE = "mesos.docker.image";

    // arguments for docker image
    public static final String DOCKER_ENTRYPOINT_ARGUMENTS = "mesos.docker.entrypoint.arguments";

    // (Required) Mesos Master URL
    public static final String MASTER_CONNECT = "mesos.master.connect";

    // Mesos task memory constraint (Default: 1024)
    public static final String EXECUTOR_MAX_MEMORY_MB = "mesos.executor.memory.mb";
    private static final double DEFAULT_EXECUTOR_MAX_MEMORY_MB = 1024; // 1GB

    // Mesos task cpu constraint (Default: 1)
    public static final String EXECUTOR_MAX_CPU_CORES = "mesos.executor.cpu.cores";
    private static final double DEFAULT_EXECUTOR_MAX_CPU_CORES = 1;

    // Mesos task max disk space (Default: 2048)
    public static final String EXECUTOR_MAX_DISK_MB = "mesos.executor.disk.mb";
    private static final double DEFAULT_EXECUTOR_MAX_DISK_MB = 2048; // 2GB

    // Slave attributes
    public static final String EXECUTOR_ATTRIBUTES = "mesos.executor.attributes";

    // Number of Samza tasks to run (Default: 1)
    public static final String EXECUTOR_TASK_COUNT = "mesos.executor.count";
    private static final int DEFAULT_EXECUTOR_TASK_COUNT = 1;

    // System user for starting the executors
    public static final String SCHEDULER_USER = "mesos.scheduler.user";
    private static final String DEFAULT_SCHEDULER_USER = "";

    // Mesos role to use for this scheduler
    public static final String SCHEDULER_ROLE = "mesos.scheduler.role";

    // Java monitoring tools (Default: true)
    public static final String SCHEDULER_JMX_ENABLED = "mesos.scheduler.jmx.enabled";
    private static final boolean DEFAULT_SCHEDULER_JMX_ENABLED = true;

    // framework (distributed system) failover time (Default: Integer.MAX_VALUE)
    public static final String SCHEDULER_FAILOVER_TIMEOUT = "mesos.scheduler.failover.timeout";
    private static final long DEFAULT_SCHEDULER_FAILOVER_TIMEOUT = Integer.MAX_VALUE;

    public MesosConfig(Config config) {
        super(config);
    }

    public String getPackagePath() {
        return get(PACKAGE_PATH);
    }

    public String getDockerImage() {
        return get(DOCKER_IMAGE);
    }

    public String getDockerEntrypointArguments() {
        return get(DOCKER_ENTRYPOINT_ARGUMENTS);
    }

    public String getMasterConnect() {
        String masterConnect = get(MASTER_CONNECT);
        if (masterConnect == null) {
            throw new SamzaException("No Mesos Master Connect in config.");
        }
        return masterConnect;
    }

    public double getExecutorMaxMemoryMb() {
        return getDouble(EXECUTOR_MAX_MEMORY_MB, DEFAULT_EXECUTOR_MAX_MEMORY_MB);
    }

    public double  getExecutorMaxCpuCores() {
        return getDouble(EXECUTOR_MAX_CPU_CORES, DEFAULT_EXECUTOR_MAX_CPU_CORES);
    }

    public double getExecutorMaxDiskMb() {
        return getDouble(EXECUTOR_MAX_DISK_MB, DEFAULT_EXECUTOR_MAX_DISK_MB);
    }

    public Map<String, String> getExecutorAttributes() {
        return subset(EXECUTOR_ATTRIBUTES, true);
    }

    public int getExecutorTaskCount() {
        return getInt(EXECUTOR_TASK_COUNT, DEFAULT_EXECUTOR_TASK_COUNT);
    }

    public String getSchedulerUser() {
        return get(SCHEDULER_USER, DEFAULT_SCHEDULER_USER);
    }

    public String getSchedulerRole() {
        return get(SCHEDULER_ROLE);
    }

    public boolean getSchedulerJmxEnabled() {
        return getBoolean(SCHEDULER_JMX_ENABLED, DEFAULT_SCHEDULER_JMX_ENABLED);
    }

    public long getSchedulerFailoverTimeout() {
        return getLong(SCHEDULER_FAILOVER_TIMEOUT, DEFAULT_SCHEDULER_FAILOVER_TIMEOUT);
    }
}
