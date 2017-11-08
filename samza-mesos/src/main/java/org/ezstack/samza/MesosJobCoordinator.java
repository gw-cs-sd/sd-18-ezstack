package org.ezstack.samza;

import org.apache.samza.coordinator.JobCoordinator;
import org.apache.samza.coordinator.JobCoordinatorListener;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.config.Config;

public class MesosJobCoordinator implements JobCoordinator {
    public MesosJobCoordinator(Config config) {
        // TODO
    }

    public void start() {
        // TODO
    }

    public void stop() {
        // TODO
    }

    public String getProcessorId() {
        // TODO
        return null;
    }

    public void setListener(JobCoordinatorListener listener) {
        // TODO
    }

    public JobModel getJobModel() {
        // TODO
        return null;
    }

}
