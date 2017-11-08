package org.ezstack.samza;

import org.apache.samza.config.Config;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.job.StreamJob;

public class MesosJob implements StreamJob {
    private MesosConfig mesosConfig;

    public MesosJob(Config config) {
        mesosConfig = new MesosConfig(config);
    }

    public StreamJob submit() {
        return null;
    }

    public StreamJob kill() {
        return null;
    }

    public ApplicationStatus waitForFinish(long l) {
        return null;
    }

    public ApplicationStatus waitForStatus(ApplicationStatus applicationStatus, long l) {
        return null;
    }

    public ApplicationStatus getStatus() {
        return null;
    }
}
