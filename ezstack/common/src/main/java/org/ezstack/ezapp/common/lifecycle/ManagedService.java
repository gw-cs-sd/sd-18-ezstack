package org.ezstack.ezapp.common.lifecycle;

public interface ManagedService {
    void start() throws Exception;

    void stop() throws Exception;
}
