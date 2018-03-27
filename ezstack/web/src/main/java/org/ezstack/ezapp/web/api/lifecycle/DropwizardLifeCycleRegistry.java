package org.ezstack.ezapp.web.api.lifecycle;

import com.google.inject.Inject;
import io.dropwizard.setup.Environment;
import org.ezstack.ezapp.common.lifecycle.LifeCycleRegistry;
import org.ezstack.ezapp.common.lifecycle.ManagedService;

import java.io.Closeable;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Implementation of {@link org.ezstack.ezapp.web.api.lifecycle.LifeCycleRegistry} for Dropwizard {@code Environment} objects.
 */
public class DropwizardLifeCycleRegistry implements LifeCycleRegistry {
    private final Environment _environment;

    @Inject
    public DropwizardLifeCycleRegistry(Environment environment) {
        _environment = checkNotNull(environment, "environment");
    }

    @Override
    public <T extends ManagedService> T manage(T managedService) {
        _environment.lifecycle().manage(new DropwizardManagedService(managedService));
        return managedService;
    }

    @Override
    public <T extends Closeable> T manage(T closeable) {
        _environment.lifecycle().manage(new DropwizardManagedCloseable(closeable));
        return closeable;
    }
}

