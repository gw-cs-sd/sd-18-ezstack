package org.ezstack.ezapp.jobmanager;

import org.ezstack.ezapp.jobmanager.api.JobManager;

import org.apache.samza.runtime.ApplicationRunnerMain;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Properties;
import java.util.UUID;

public class DefaultJobManager implements JobManager {
    private static final String CONFIG_FACTORY_CMD = "--config-factory=org.apache.samza.config.factories.PropertiesConfigFactory";
    private static final String PREFIX_CONFIG_PATH_CMD = "--config-path=";
    private static final String SHUTDOWN_CMD = "--operation=kill";
    private static final String JOB_ID = "job.id";

    @Override
    public Properties create(Properties properties) throws Exception {
        String id = getOrCreateId(properties);
        File temp = File.createTempFile(id, ".properties");
        properties.store(new FileOutputStream(temp), null);
        String[] args = {CONFIG_FACTORY_CMD, getConfigPath(temp.getAbsolutePath())};
        ApplicationRunnerMain.main(args);
        return properties;
    }

    @Override
    public void shutdown(Properties properties) throws Exception {
        File temp = File.createTempFile(properties.getProperty(JOB_ID), ".properties");
        properties.store(new FileOutputStream(temp), null);
        String[] args = {CONFIG_FACTORY_CMD, getConfigPath(temp.getAbsolutePath()), SHUTDOWN_CMD};
        ApplicationRunnerMain.main(args);
    }

    @Override
    public String getOrCreateId(Properties properties) {
        String id = properties.getProperty(JOB_ID);

        if (id == null) {
            id = UUID.randomUUID().toString();
            properties.setProperty(JOB_ID, id);
        }

        return id;
    }

    private String getConfigPath(String filePath) {
        return PREFIX_CONFIG_PATH_CMD + "file://" + filePath;
    }
}
