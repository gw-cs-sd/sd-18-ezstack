package org.ezstack.samza;

import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.samza.config.Config;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.job.StreamJob;

public class MesosJob implements StreamJob {
    private ApplicationStatus applicationStatus;
    private MesosConfig mesosConfig;
    private MesosSchedulerDriver mesosSchedulerDriver;
    private SamzaScheduler samzaScheduler;
    private SamzaExecutor samzaExecutor;

    public MesosJob(Config config) {
        mesosConfig = new MesosConfig(config);
        samzaScheduler = new SamzaScheduler(); // Constructor will change
        mesosSchedulerDriver = new MesosSchedulerDriver(samzaScheduler,
                getFrameWorkInfo(), mesosConfig.getMasterConnect());
        applicationStatus = ApplicationStatus.New;
    }

    public StreamJob submit() {
        // TODO
        applicationStatus = ApplicationStatus.Running;
        return this;
    }

    public StreamJob kill() {
        // TODO
        return this;
    }

    public ApplicationStatus waitForFinish(long l) {
        return null;
    }

    public ApplicationStatus waitForStatus(ApplicationStatus applicationStatus, long l) {
        return null;
    }

    public ApplicationStatus getStatus() {
        return applicationStatus;
    }

    private static Protos.FrameworkInfo getFrameWorkInfo() {
        return null;
    }
}
