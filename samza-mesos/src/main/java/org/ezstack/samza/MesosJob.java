package org.ezstack.samza;

import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.samza.config.Config;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.job.StreamJob;

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
            } catch (Exception e) { /* do nothing */ }
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
            } catch (Exception e) { /* do nothing */ }
        }

        return getStatus();
    }

    public ApplicationStatus getStatus() {
        return ApplicationStatus.New; // TODO: Fix ME
    }

    private Protos.FrameworkInfo getFrameWorkInfo() {
        Protos.FrameworkInfo.Builder builder = Protos.FrameworkInfo.newBuilder();
        builder.setFailoverTimeout(mesosConfig.getSchedulerFailoverTimeout());
        builder.setUser(mesosConfig.getSchedulerUser());
        builder.setName(mesosConfig.getFrameworkName());
        return builder.build();
    }
}
