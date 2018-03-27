package org.ezstack.ezapp.web.api.lifecycle;

import com.google.common.base.MoreObjects;
import com.google.common.io.Closeables;
import io.dropwizard.lifecycle.Managed;

import java.io.Closeable;

/**
 * Adapts the Dropwizard {@link Managed} interface for a {@link Closeable}.  This allows Dropwizard to
 * cleanup resources without those servers requiring a direct dependency on Dropwizard.
 */
public class DropwizardManagedCloseable implements Managed {
    private final Closeable _closeable;

    public DropwizardManagedCloseable(Closeable closeable) {
        _closeable = closeable;
    }

    @Override
    public void start() throws Exception {
        // do nothing
    }

    @Override
    public void stop() throws Exception {
        Closeables.close(_closeable, true);
    }

    // For debugging
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).addValue(_closeable).toString();
    }
}

