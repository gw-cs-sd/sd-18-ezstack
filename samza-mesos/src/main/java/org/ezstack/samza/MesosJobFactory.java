package org.ezstack.samza;

import org.apache.samza.job.StreamJob;
import org.apache.samza.job.StreamJobFactory;
import org.apache.samza.config.Config;

public class MesosJobFactory implements StreamJobFactory{
    public StreamJob getJob(Config config) {
        return new MesosJob(config);
    }
}
