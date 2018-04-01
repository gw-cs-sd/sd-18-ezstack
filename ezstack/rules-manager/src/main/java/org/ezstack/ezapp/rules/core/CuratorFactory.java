package org.ezstack.ezapp.rules.core;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.WatcherRemoveCuratorFramework;
import org.apache.curator.framework.api.*;
import org.apache.curator.framework.api.transaction.CuratorMultiTransaction;
import org.apache.curator.framework.api.transaction.CuratorTransaction;
import org.apache.curator.framework.api.transaction.TransactionOp;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.schema.SchemaSet;
import org.apache.curator.framework.state.ConnectionStateErrorPolicy;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.EnsurePath;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.TimeUnit;

/**
 * This is a factory that will allow multiple services to utilize the same {@link CuratorFramework}, as is recommended
 * in their documentation. This has one major vulnerability, which is that if other services are still using the shared
 * framework after another calls {@link SharedCuratorFramework#close()}, then it will still be operational to the one
 * that already thought it closed.
 */
public class CuratorFactory {

    private final static int BASE_RETRY_SLEEP_TYPE_IN_MS = 1000;
    private final static int MAX_CURATOR_RETRIES = 3;

    private final SharedCuratorFramework _sharedCuratorFramework;

    @Inject
    public CuratorFactory(@Named("zookeeperHosts") String zookeeperHosts) {
        checkNotNull(zookeeperHosts, "zookeeperHosts");
        _sharedCuratorFramework = new SharedCuratorFramework(zookeeperHosts);

    }

    synchronized public CuratorFramework getStartedCuratorFramework() {
        _sharedCuratorFramework.start();
        return _sharedCuratorFramework;
    }


    private static class SharedCuratorFramework implements CuratorFramework {

        private volatile CuratorFramework _curatorFramework;
        private volatile int _refCount;
        private volatile String _zookeeperHosts;


        SharedCuratorFramework(String zookeeperHosts) {
            _zookeeperHosts = zookeeperHosts;
            _refCount = 0;
        }

        @Override
        synchronized public void start() {
            if (_refCount == 0) {
                _curatorFramework = CuratorFrameworkFactory.newClient(_zookeeperHosts, new ExponentialBackoffRetry(BASE_RETRY_SLEEP_TYPE_IN_MS, MAX_CURATOR_RETRIES));
                _curatorFramework.start();
            }

            _refCount++;
        }

        @Override
        synchronized public void close() {
            _refCount--;
            if (_refCount == 0) {
                _curatorFramework.close();
                _curatorFramework = null;
            }
        }

        @Override
        public CuratorFrameworkState getState() {
            return _curatorFramework.getState();
        }

        @Override
        public boolean isStarted() {
            return _curatorFramework.isStarted();
        }

        @Override
        public CreateBuilder create() {
            return _curatorFramework.create();
        }

        @Override
        public DeleteBuilder delete() {
            return _curatorFramework.delete();
        }

        @Override
        public ExistsBuilder checkExists() {
            return _curatorFramework.checkExists();
        }

        @Override
        public GetDataBuilder getData() {
            return _curatorFramework.getData();
        }

        @Override
        public SetDataBuilder setData() {
            return _curatorFramework.setData();
        }

        @Override
        public GetChildrenBuilder getChildren() {
            return _curatorFramework.getChildren();
        }

        @Override
        public GetACLBuilder getACL() {
            return _curatorFramework.getACL();
        }

        @Override
        public SetACLBuilder setACL() {
            return _curatorFramework.setACL();
        }

        @Override
        public ReconfigBuilder reconfig() {
            return _curatorFramework.reconfig();
        }

        @Override
        public GetConfigBuilder getConfig() {
            return _curatorFramework.getConfig();
        }

        @Override
        public CuratorTransaction inTransaction() {
            return null;
        }

        @Override
        public CuratorMultiTransaction transaction() {
            return _curatorFramework.transaction();
        }

        @Override
        public TransactionOp transactionOp() {
            return _curatorFramework.transactionOp();
        }

        @Override
        public void sync(String path, Object backgroundContextObject) {
            _curatorFramework.sync(path, backgroundContextObject);
        }

        @Override
        public void createContainers(String path) throws Exception {
            _curatorFramework.createContainers(path);
        }

        @Override
        public SyncBuilder sync() {
            return _curatorFramework.sync();
        }

        @Override
        public RemoveWatchesBuilder watches() {
            return _curatorFramework.watches();
        }

        @Override
        public Listenable<ConnectionStateListener> getConnectionStateListenable() {
            return _curatorFramework.getConnectionStateListenable();
        }

        @Override
        public Listenable<CuratorListener> getCuratorListenable() {
            return _curatorFramework.getCuratorListenable();
        }

        @Override
        public Listenable<UnhandledErrorListener> getUnhandledErrorListenable() {
            return _curatorFramework.getUnhandledErrorListenable();
        }

        @Override
        public CuratorFramework nonNamespaceView() {
            return _curatorFramework.nonNamespaceView();
        }

        @Override
        public CuratorFramework usingNamespace(String newNamespace) {
            return _curatorFramework.usingNamespace(newNamespace);
        }

        @Override
        public String getNamespace() {
            return _curatorFramework.getNamespace();
        }

        @Override
        public CuratorZookeeperClient getZookeeperClient() {
            return _curatorFramework.getZookeeperClient();
        }

        @Override
        public EnsurePath newNamespaceAwareEnsurePath(String path) {
            return _curatorFramework.newNamespaceAwareEnsurePath(path);
        }

        @Override
        public void clearWatcherReferences(Watcher watcher) {
            _curatorFramework.clearWatcherReferences(watcher);
        }

        @Override
        public boolean blockUntilConnected(int maxWaitTime, TimeUnit units) throws InterruptedException {
            return _curatorFramework.blockUntilConnected(maxWaitTime, units);
        }

        @Override
        public void blockUntilConnected() throws InterruptedException {
            _curatorFramework.blockUntilConnected();
        }

        @Override
        public WatcherRemoveCuratorFramework newWatcherRemoveCuratorFramework() {
            return _curatorFramework.newWatcherRemoveCuratorFramework();
        }

        @Override
        public ConnectionStateErrorPolicy getConnectionStateErrorPolicy() {
            return _curatorFramework.getConnectionStateErrorPolicy();
        }

        @Override
        public QuorumVerifier getCurrentConfig() {
            return _curatorFramework.getCurrentConfig();
        }

        @Override
        public SchemaSet getSchemaSet() {
            return _curatorFramework.getSchemaSet();
        }

        @Override
        public boolean isZk34CompatibilityMode() {
            return _curatorFramework.isZk34CompatibilityMode();
        }
    }
}
