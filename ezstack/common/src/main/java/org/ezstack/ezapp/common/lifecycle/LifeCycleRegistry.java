package org.ezstack.ezapp.common.lifecycle;

import java.io.Closeable;

/**
 * Registry that promises to call {@link ManagedService#start()} and  {@link ManagedService#stop()} at the appropriate time for
 * each registered instance of {@link ManagedService}.
 */
public interface LifeCycleRegistry {
    <T extends ManagedService> T manage(T managedService);
    <T extends Closeable> T manage(T closeable);
}
