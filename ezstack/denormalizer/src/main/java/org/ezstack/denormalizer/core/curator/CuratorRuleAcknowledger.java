package org.ezstack.denormalizer.core.curator;

import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.AbstractService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import static com.google.common.base.Preconditions.checkNotNull;

public class CuratorRuleAcknowledger extends AbstractService {

    private final static int BASE_RETRY_SLEEP_TYPE_IN_MS = 1000;
    private final static int MAX_CURATOR_RETRIES = 3;

    private final String _zookeeperHosts;

    private CuratorFramework _client;

    public CuratorRuleAcknowledger(String zookeeperHosts) {
        _zookeeperHosts = checkNotNull(zookeeperHosts, "zookeeperHosts");
    }

    @Override
    protected void doStart() {
        try {
            _client = CuratorFrameworkFactory.newClient(_zookeeperHosts,
                    new ExponentialBackoffRetry(BASE_RETRY_SLEEP_TYPE_IN_MS, MAX_CURATOR_RETRIES));
            _client.start();
        } catch (Exception e) {
            notifyFailed(e);
            throw e;
        }

        notifyStarted();
    }

    @Override
    protected void doStop() {
        try {
            _client.close();
        } catch (Exception e) {
            notifyFailed(e);
            throw e;
        }

        notifyStopped();
    }
}
