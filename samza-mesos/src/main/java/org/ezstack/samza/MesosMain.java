package org.ezstack.samza;

import joptsimple.OptionSet;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.samza.util.CommandLine;
import org.apache.samza.config.Config;

public class MesosMain {
    public static void main (String[] args) throws Exception {
        CommandLine cmdLine = new CommandLine();
        OptionSet options = cmdLine.parser().parse(args);
        Config config = cmdLine.loadConfig(options);

        MesosJob mesosJob = new MesosJob(config);
        MesosSchedulerDriver driver = mesosJob.getMesosSchedulerDriver();
        int status = driver.run() == Protos.Status.DRIVER_STOPPED ? 0 : 1;
        driver.stop();
        Thread.sleep(500);
        System.exit(status);
    }
}
