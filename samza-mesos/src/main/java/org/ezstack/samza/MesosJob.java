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

import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.samza.config.Config;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.job.StreamJob;

import java.util.Calendar;

public class MesosJob implements StreamJob {
    private MesosConfig mesosConfig;
    private MesosSchedulerDriver mesosSchedulerDriver;
    private SamzaScheduler samzaScheduler;

    public MesosJob(Config config) {
        mesosConfig = new MesosConfig(config);
        samzaScheduler = new SamzaScheduler(mesosConfig);
        mesosSchedulerDriver = new MesosSchedulerDriver(samzaScheduler,
                getFrameWorkInfo(), mesosConfig.getMasterConnect());
    }

    public StreamJob submit() {
        mesosSchedulerDriver.run();
        return this;
    }

    public StreamJob kill() {
        mesosSchedulerDriver.stop();
        return this;
    }

    public ApplicationStatus waitForFinish(long timeOutMs) {
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < timeOutMs) {
            ApplicationStatus s = getStatus();
            if (s.equals(ApplicationStatus.SuccessfulFinish) ||
                    s.equals(ApplicationStatus.UnsuccessfulFinish)) {
                return s;
            }
            try {
                Thread.sleep(1000); // 1 ms sleep
            } catch (InterruptedException e) { /* do nothing */ }
        }

        return ApplicationStatus.Running;
    }

    public ApplicationStatus waitForStatus(ApplicationStatus applicationStatus, long timeOutMs) {
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < timeOutMs) {
            ApplicationStatus s = getStatus();
            if (s.equals(applicationStatus)) {
                return s;
            }
            try {
                Thread.sleep(1000); // 1 ms sleep
            } catch (InterruptedException e) { /* do nothing */ }
        }

        return getStatus();
    }

    public ApplicationStatus getStatus() {
        return ApplicationStatus.New; // TODO: Fix ME
    }

    private Protos.FrameworkInfo getFrameWorkInfo() {
        String frameworkName = mesosConfig.getName().get();
        return Protos.FrameworkInfo.newBuilder()
                .setFailoverTimeout(mesosConfig.getSchedulerFailoverTimeout())
                .setUser(mesosConfig.getSchedulerUser())
                .setName(frameworkName)
                .setId(Protos.FrameworkID.newBuilder()
                        .setValue(frameworkName + "-" + Calendar.getInstance().getTimeInMillis())
                        .build())
                .build();
    }
}
