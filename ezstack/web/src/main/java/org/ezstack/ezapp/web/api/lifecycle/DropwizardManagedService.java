package org.ezstack.ezapp.web.api.lifecycle;

import io.dropwizard.lifecycle.Managed;
import org.ezstack.ezapp.common.lifecycle.ManagedService;

public class DropwizardManagedService implements Managed {

    private final ManagedService _managed;

    public DropwizardManagedService(ManagedService managed) {
        _managed = managed;
    }

    @Override
    public void start() throws Exception {
        _managed.start();
    }

    @Override
    public void stop() throws Exception {
        _managed.stop();
    }
}
