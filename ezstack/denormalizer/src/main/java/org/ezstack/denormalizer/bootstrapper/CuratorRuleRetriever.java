package org.ezstack.denormalizer.bootstrapper;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.ezstack.ezapp.datastore.api.Rule;

import java.util.Set;

public class CuratorRuleRetriever {

    private final static int BASE_RETRY_SLEEP_TYPE_IN_MS = 1000;
    private final static int MAX_CURATOR_RETRIES = 3;
    private final static ObjectMapper MAPPER = new ObjectMapper();

    public static Set<Rule> getRulesForJobID(String zookeeperHosts, String jobId) {

        CuratorFramework client = CuratorFrameworkFactory.newClient(zookeeperHosts,
                new ExponentialBackoffRetry(BASE_RETRY_SLEEP_TYPE_IN_MS, MAX_CURATOR_RETRIES));
        client.start();
        try {
            Set<Rule> rules = MAPPER.readValue(client.getData()
                    .forPath(ZKPaths.makePath("/bootstrapper", jobId)), new TypeReference<Set<Rule>>() {});
            client.close();
            return rules;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
