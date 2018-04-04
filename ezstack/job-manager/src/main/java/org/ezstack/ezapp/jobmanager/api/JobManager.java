package org.ezstack.ezapp.jobmanager.api;

import java.util.Properties;

public interface JobManager {
    public Properties create(Properties properties) throws Exception;

    public void shutdown(Properties properties) throws Exception;

    public String getOrCreateId(Properties properties);
}
