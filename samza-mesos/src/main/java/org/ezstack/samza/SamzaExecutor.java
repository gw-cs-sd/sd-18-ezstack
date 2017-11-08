package org.ezstack.samza;

import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;

public class SamzaExecutor implements Executor {
    public void registered(ExecutorDriver executorDriver, Protos.ExecutorInfo executorInfo, Protos.FrameworkInfo frameworkInfo, Protos.SlaveInfo slaveInfo) {

    }

    public void reregistered(ExecutorDriver executorDriver, Protos.SlaveInfo slaveInfo) {

    }

    public void disconnected(ExecutorDriver executorDriver) {

    }

    public void launchTask(ExecutorDriver executorDriver, Protos.TaskInfo taskInfo) {

    }

    public void killTask(ExecutorDriver executorDriver, Protos.TaskID taskID) {

    }

    public void frameworkMessage(ExecutorDriver executorDriver, byte[] bytes) {

    }

    public void shutdown(ExecutorDriver executorDriver) {

    }

    public void error(ExecutorDriver executorDriver, String s) {

    }
}
