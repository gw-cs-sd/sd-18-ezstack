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

import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.ShellCommandConfig;
import org.apache.samza.job.CommandBuilder;
import org.apache.samza.job.ShellCommandBuilder;
import org.apache.samza.serializers.model.SamzaObjectMapper;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
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
                String url = offer.getUrl().getAddress().getIp() + ":" + offer.getUrl().getAddress().getPort();
                LOG.info("Offer id: {}, Url: {}", containerId, url);

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
        String cmd = commandBuilder.buildCommand();
        LOG.info("exec command: {}", cmd);


        return Protos.CommandInfo.newBuilder()
                .addUris(Protos.CommandInfo.URI.newBuilder()
                        .setValue(mesosConfig.getPackagePath())
                        .setExtract(true)
                        .build())
                .setValue(cmd)
                .setEnvironment(getBuiltMesosEnvironment(commandBuilder.buildEnvironment()))
                .build();
    }

//    private String buildCmd() {
//        // Figure out if framework is deployed else where
//        String fwkPath = mesosConfig.get(JobConfig.SAMZA_FWK_PATH(), "");
//        String fwkVersion = mesosConfig.get(JobConfig.SAMZA_FWK_VERSION());
//        if (fwkVersion == null || fwkVersion.isEmpty()) {
//            fwkVersion = "STABLE";
//        }
//
//        String cmdExec = "./bin/run-jc.sh"; // default location
//        if (!fwkPath.isEmpty()) {
//            cmdExec = fwkPath + "/" + fwkVersion + "/bin/run-jc.sh";
//        }
//        LOG.info("Build cmd path: {}", cmdExec);
//
//        return cmdExec;
//    }

    private CommandBuilder getSamzaCommandBuilder(String containerId) {
        String fwkPath = mesosConfig.get(JobConfig.SAMZA_FWK_PATH(), "");
        String fwkVersion = mesosConfig.get(JobConfig.SAMZA_FWK_VERSION());
        if (fwkVersion == null || fwkVersion.isEmpty()) {
            fwkVersion = "STABLE";
        }

        String cmdPath = ".";
        if (!fwkPath.isEmpty()) {
            cmdPath = fwkPath + "/" + fwkVersion;
        }

        CommandBuilder commandBuilder = new ShellCommandBuilder();
        commandBuilder.setConfig(mesosConfig)
                .setId(containerId)
                .setCommandPath(cmdPath);
        try {
            commandBuilder.setUrl(new URL("http://ezstack12.parmer.seas.gwu.edu:8080"));
        } catch (MalformedURLException e) {
            LOG.error("couldn't set url: {}", e.getMessage());
        }

        return commandBuilder;
    }

    private Protos.Environment getBuiltMesosEnvironment(Map<String, String> envMap) {
        Protos.Environment.Builder envBuilder = Protos.Environment.newBuilder();
        String mem = "" + mesosConfig.getExecutorMaxMemoryMb();

        MapConfig cordSystemConfig = Util.buildCoordinatorStreamConfig(mesosConfig);
        Map<String, String> map = new HashMap<>();
        map.putAll(envMap);
        map.put("JAVA_HEAP_OPTS", "-Xms" + mem + "M -Xmx" + mem + "M");
//        map.put("JOB_LIB_DIR", "./lib");
        try {
            map.put(ShellCommandConfig.ENV_COORDINATOR_SYSTEM_CONFIG(), Util.envVarEscape(SamzaObjectMapper
                    .getObjectMapper().writeValueAsString(cordSystemConfig)));
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (Map.Entry<String, String> entry: map.entrySet()) {
            LOG.info("export {} = {}", entry.getKey(), entry.getValue());
            envBuilder.addVariables(Protos.Environment.Variable.newBuilder()
                    .setName(entry.getKey())
                    .setValue(entry.getValue())
                    .build());
        }

        return envBuilder.build();
    }
}
