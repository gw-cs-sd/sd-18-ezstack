package org.ezstack.samza;

import org.apache.samza.config.Config;
import org.apache.samza.coordinator.JobCoordinator;
import org.apache.samza.coordinator.JobCoordinatorFactory;

public class MesosJobCoordinatorFactory implements JobCoordinatorFactory {
    public JobCoordinator getJobCoordinator(Config config) {
        return new MesosJobCoordinator(config);
    }
}
