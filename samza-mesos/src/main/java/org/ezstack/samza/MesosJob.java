
package org.ezstack.samza;

import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.samza.config.Config;
import java.util.Calendar;

public class MesosJob {
    private MesosConfig mesosConfig;
    private MesosSchedulerDriver mesosSchedulerDriver;
    private SamzaScheduler samzaScheduler;

    public MesosJob(Config config) {
        mesosConfig = new MesosConfig(config);
        samzaScheduler = new SamzaScheduler(mesosConfig);
        mesosSchedulerDriver = new MesosSchedulerDriver(samzaScheduler,
                getFrameWorkInfo(), mesosConfig.getMasterConnect());
    }

    public MesosSchedulerDriver getMesosSchedulerDriver() {
        return mesosSchedulerDriver;
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
