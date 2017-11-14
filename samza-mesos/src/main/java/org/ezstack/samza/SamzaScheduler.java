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

import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import org.apache.samza.config.TaskConfig;
import org.apache.samza.job.CommandBuilder;
import org.apache.samza.job.ShellCommandBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

                String containerId = "samza-task-" + taskId.getValue();
                // TODO: Docker stuff Maybe

                // Create Mesos Task To Run
                Protos.TaskInfo task = Protos.TaskInfo.newBuilder()
                        .setName(containerId)
                        .setTaskId(taskId)
                        .setSlaveId(offer.getSlaveId())
                        .addResources(getResourceBuilder("cpus", mesosConfig.getExecutorMaxCpuCores()))
                        .addResources(getResourceBuilder("mem", mesosConfig.getExecutorMaxMemoryMb()))
                        .addResources(getResourceBuilder("disk", mesosConfig.getExecutorMaxDiskMb()))
                        .setCommand(getCommand(containerId))
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

    private Protos.Resource.Builder getResourceBuilder(String resource, double value) {
        return Protos.Resource.newBuilder()
                .setName(resource)
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder()
                        .setValue(value));
    }

    private Protos.CommandInfo getCommand(String containerId) {
        CommandBuilder commandBuilder = getSamzaCommandBuilder(containerId);
        return Protos.CommandInfo.newBuilder()
                .addUris(Protos.CommandInfo.URI.newBuilder()
                        .setValue(mesosConfig.getPackagePath())
                        .setExtract(true)
                        .build())
                .setValue(commandBuilder.buildCommand())
                //.setEnvironment(getBuiltMesosEnvironment(commandBuilder.buildEnvironment()))
                .build();
    }

    private CommandBuilder getSamzaCommandBuilder(String containerId) {
        CommandBuilder ret = null;
        TaskConfig tc = new TaskConfig(mesosConfig);
        String cmdBuilderClassName = tc.getCommandClass(ShellCommandBuilder.class.getName());
        try {
            ret = (CommandBuilder)Class.forName(cmdBuilderClassName).newInstance();
            ret.setConfig(mesosConfig);
            ret.setId(containerId);
        } catch (ClassNotFoundException e) {
            LOG.error(e.getMessage());
        } catch (InstantiationException e) {
            LOG.error(e.getMessage());
        } catch (IllegalAccessException e) {
            LOG.error(e.getMessage());
        }

        return ret;
    }

    private Protos.Environment getBuiltMesosEnvironment(Map<String, String> envMap) {
        Protos.Environment.Builder envBuilder = Protos.Environment.newBuilder();

        for (Map.Entry<String, String> entry: envMap.entrySet()) {
            envBuilder.addVariables(Protos.Environment.Variable.newBuilder()
                    .setName(entry.getKey())
                    .setValue(entry.getValue())
                    .build());
        }

        return envBuilder.build();
    }
}
