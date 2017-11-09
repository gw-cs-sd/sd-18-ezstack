package org.ezstack.samza;

import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SamzaExecutor implements Executor {
    private static final Logger LOG = LoggerFactory.getLogger(SamzaExecutor.class);

    public void registered(ExecutorDriver executorDriver, Protos.ExecutorInfo executorInfo,
                           Protos.FrameworkInfo frameworkInfo, Protos.SlaveInfo slaveInfo) {
        LOG.info("registered slave on " + slaveInfo.getHostname());
    }

    public void reregistered(ExecutorDriver executorDriver, Protos.SlaveInfo slaveInfo) {
        LOG.info("reregistered slave on " + slaveInfo.getHostname());
    }

    public void disconnected(ExecutorDriver executorDriver) {
        LOG.info("disconnected");
    }

    public void launchTask(ExecutorDriver executorDriver, Protos.TaskInfo taskInfo) {

    }

    public void killTask(ExecutorDriver executorDriver, Protos.TaskID taskID) {
        LOG.info("task " + taskID.getValue() + " killed");
    }

    public void frameworkMessage(ExecutorDriver executorDriver, byte[] bytes) {
        LOG.info("framework message: " + new String(bytes));
    }

    public void shutdown(ExecutorDriver executorDriver) {
        LOG.info("shutdown");
    }

    public void error(ExecutorDriver executorDriver, String s) {
        LOG.error("error: " + s);
    }
}
