package org.ezstack.samza;

import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class SamzaScheduler implements Scheduler {
    private static final Logger LOG = LoggerFactory.getLogger(SamzaScheduler.class);
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

    public void resourceOffers(SchedulerDriver schedulerDriver, List<Protos.Offer> list) {

    }

    public void offerRescinded(SchedulerDriver schedulerDriver, Protos.OfferID offerID) {

    }

    public void statusUpdate(SchedulerDriver schedulerDriver, Protos.TaskStatus taskStatus) {

    }

    public void frameworkMessage(SchedulerDriver schedulerDriver, Protos.ExecutorID executorID, Protos.SlaveID slaveID, byte[] bytes) {
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
