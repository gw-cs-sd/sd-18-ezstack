package org.ezstack.ezapp.common.lifecycle;

import com.google.common.base.MoreObjects;
import com.google.common.util.concurrent.Service;

public class GuavaManagedService implements ManagedService {

    private final Service _service;

    public GuavaManagedService(Service service) {
        _service = service;
    }

    @Override
    public void start() throws Exception {
        _service.startAsync().awaitRunning();
    }

    @Override
    public void stop() throws Exception {
        _service.stopAsync().awaitTerminated();
    }

    // For debugging
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).addValue(_service).toString();
    }
}
