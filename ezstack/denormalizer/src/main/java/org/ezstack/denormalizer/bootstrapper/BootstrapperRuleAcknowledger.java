package org.ezstack.denormalizer.bootstrapper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BootstrapperRuleAcknowledger {

    private static final Logger LOG = LoggerFactory.getLogger(BootstrapperRuleAcknowledger.class);

    private final static int BASE_RETRY_SLEEP_TYPE_IN_MS = 1000;
    private final static int MAX_CURATOR_RETRIES = 5;

    public static void acknowledgeBootstrappingComplete(String zkHosts, String bootstrapPath, String partitionId, String jobId) {
        CuratorFramework client = CuratorFrameworkFactory.newClient(zkHosts, new ExponentialBackoffRetry(BASE_RETRY_SLEEP_TYPE_IN_MS, MAX_CURATOR_RETRIES));
        try {
            client.start();
            client.create()
                    .forPath(ZKPaths.makePath(bootstrapPath, jobId, partitionId));

        } catch (Exception e) {
            if (e instanceof KeeperException.NodeExistsException) {
                LOG.error("Already acknowledged parition id {} for job id {}", partitionId, jobId);
                return;
            }

            LOG.error(e.toString());
            throw new RuntimeException("Failed to acknowledge rule", e);
        } finally {
            client.close();
        }
    }
}
