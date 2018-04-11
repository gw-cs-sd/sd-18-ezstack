package org.ezstack.denormalizer.bootstrapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.MoreObjects;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.AbstractService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.ezstack.denormalizer.model.RuleIndexPair;
import org.ezstack.denormalizer.model.RuleIndexer;
import org.ezstack.ezapp.datastore.api.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkNotNull;

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
