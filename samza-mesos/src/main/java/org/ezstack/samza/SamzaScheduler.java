package org.ezstack.samza;

import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class SamzaScheduler implements Scheduler {
    private static final Logger LOG = LoggerFactory.getLogger(SamzaScheduler.class);
    private final List<String> pendingInstance = new ArrayList<>();
    private final List<String> runningInstance = new ArrayList<>();
    private final AtomicInteger taskIDGenerator = new AtomicInteger();

    private MesosConfig mesosConfig;

    public SamzaScheduler(MesosConfig config) {
        mesosConfig = config;
    }

    public void registered(SchedulerDriver schedulerDriver, Protos.FrameworkID frameworkID, Protos.MasterInfo masterInfo) {
        LOG.info("Registered " + frameworkID);
    }

    public void reregistered(SchedulerDriver schedulerDriver, Protos.MasterInfo masterInfo) {
        LOG.info("Reregistered");
    }

    public void resourceOffers(SchedulerDriver schedulerDriver, List<Protos.Offer> offers) {
        for (Protos.Offer offer: offers) {
            List<Protos.TaskInfo> tasks = new ArrayList<>();
            List<Protos.OfferID> offerIdS = new ArrayList<>();

            if (runningInstance.size() + pendingInstance.size() < mesosConfig.getExecutorTaskCount()) {
                // Generate Unique Task ID
                Protos.TaskID taskId = Protos.TaskID.newBuilder()
                        .setValue(Integer.toString(taskIDGenerator.incrementAndGet())).build();

                // Launch Task
                pendingInstance.add(taskId.getValue());

                // TODO: Docker stuff

                // Create Task To Run
                Protos.TaskInfo task = Protos.TaskInfo.newBuilder()
                        .setName("task " + taskId.getValue())
                        .setTaskId(taskId)
                        .setSlaveId(offer.getSlaveId())
                        .addResources(Protos.Resource.newBuilder()
                                .setName("cpus")
                                .setType(Protos.Value.Type.SCALAR)
                                .setScalar(Protos.Value.Scalar.newBuilder()
                                        .setValue(mesosConfig.getExecutorMaxCpuCores())))
                        .addResources(Protos.Resource.newBuilder()
                                .setName("mem")
                                .setType(Protos.Value.Type.SCALAR)
                                .setScalar(Protos.Value.Scalar.newBuilder()
                                        .setValue(mesosConfig.getExecutorMaxMemoryMb())))
                        .addResources(Protos.Resource.newBuilder()
                                .setName("disk")
                                .setType(Protos.Value.Type.SCALAR)
                                .setScalar(Protos.Value.Scalar.newBuilder()
                                        .setValue(mesosConfig.getExecutorMaxDiskMb())))
                        .setCommand(Protos.CommandInfo.newBuilder()
                                .addUris(Protos.CommandInfo.URI.newBuilder()
                                        .setValue(mesosConfig.getPackagePath())
                                        .setExtract(true)
                                        .build())
                                .setValue(null) // TODO: fix value to the command value
                                .build())
                        .build();
                tasks.add(task);
            }
            offerIdS.add(offer.getId());
            Protos.Filters filter = Protos.Filters.newBuilder().setRefuseSeconds(1).build();
            schedulerDriver.launchTasks(offerIdS, tasks, filter);
        }
    }

    public void offerRescinded(SchedulerDriver schedulerDriver, Protos.OfferID offerID) {
        LOG.info("offer rescinded");
    }

    public void statusUpdate(SchedulerDriver schedulerDriver, Protos.TaskStatus taskStatus) {
        final String taskId = taskStatus.getTaskId().getValue();

        switch (taskStatus.getState()) {
            case TASK_RUNNING:
                pendingInstance.remove(taskId);
                runningInstance.add(taskId);
                break;
            case TASK_LOST: // fall through
            case TASK_KILLED:
            case TASK_FAILED:
            case TASK_FINISHED:
                pendingInstance.remove(taskId);
                runningInstance.remove(taskId);
                break;
        }

        LOG.info("Running Instance Count: {}, Pending Instance Count: {}", runningInstance.size(), pendingInstance.size());
    }

    public void frameworkMessage(SchedulerDriver schedulerDriver, Protos.ExecutorID executorID,
                                 Protos.SlaveID slaveID, byte[] bytes) {
        LOG.info("Framework (scheduler) message: " + new String(bytes));
    }

    public void disconnected(SchedulerDriver schedulerDriver) {
        LOG.info("Framework has been disconnected.");
    }

    public void slaveLost(SchedulerDriver schedulerDriver, Protos.SlaveID slaveID) {
        LOG.error("Slave " + slaveID.getValue() + " has been lost.");
    }

    public void executorLost(SchedulerDriver schedulerDriver, Protos.ExecutorID executorID, Protos.SlaveID slaveID, int i) {
        LOG.error("Executor " + executorID.getValue() + " on Slave " + slaveID.getValue() + " has been lost.");
    }

    public void error(SchedulerDriver schedulerDriver, String s) {
        LOG.error("Error Report: " + s);
    }
}
