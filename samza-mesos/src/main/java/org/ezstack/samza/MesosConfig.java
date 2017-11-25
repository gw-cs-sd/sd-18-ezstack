
package org.ezstack.samza;

import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;

import java.util.Map;

public class MesosConfig extends JobConfig {

    // (Required) the job package URI (file, http, hdfs)
    public static final String PACKAGE_PATH = "mesos.package.path";

    // (Required) Mesos Master URL
    public static final String MASTER_CONNECT = "mesos.master.connect";

    // (Required) Mesos Command
    public static final String COMMAND = "mesos.command";

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

    // framework (distributed system) failover time (Default: Integer.MAX_VALUE)
    public static final String SCHEDULER_FAILOVER_TIMEOUT = "mesos.scheduler.failover.timeout";
    private static final long DEFAULT_SCHEDULER_FAILOVER_TIMEOUT = Integer.MAX_VALUE;

    public MesosConfig(Config config) {
        super(config);
    }

    public String getPackagePath() {
        return get(PACKAGE_PATH);
    }

    public String getMasterConnect() {
        String masterConnect = get(MASTER_CONNECT);
        if (masterConnect == null) {
            throw new SamzaException("No Mesos Master Connect in config.");
        }
        return masterConnect;
    }

    public String getCommand() {
        String cmd = get(COMMAND);
        if (cmd == null || cmd.isEmpty()) {
            throw new SamzaException("No Command in config.");
        }
        return cmd;
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

    public int getExecutorTaskCount() {
        return getInt(EXECUTOR_TASK_COUNT, DEFAULT_EXECUTOR_TASK_COUNT);
    }

    public String getSchedulerUser() {
        return get(SCHEDULER_USER, DEFAULT_SCHEDULER_USER);
    }

    public long getSchedulerFailoverTimeout() {
        return getLong(SCHEDULER_FAILOVER_TIMEOUT, DEFAULT_SCHEDULER_FAILOVER_TIMEOUT);
    }
}
