package org.ezstack.denormalizer.core;

import com.google.common.collect.*;
import org.apache.commons.collections4.KeyValue;
import org.apache.commons.collections4.keyvalue.DefaultKeyValue;
import org.apache.samza.config.Config;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.apache.samza.task.TaskContext;
import org.ezstack.denormalizer.core.curator.CuratorRuleIndexer;
import org.ezstack.denormalizer.model.*;
import org.ezstack.ezapp.datastore.api.Document;
import org.ezstack.ezapp.datastore.api.Query;
import org.ezstack.ezapp.datastore.api.QueryHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

public class DocumentMessageMapper implements FlatMapFunction<DocumentChangePair, DocumentMessage> {

    private static final Logger log = LoggerFactory.getLogger(DocumentMessageMapper.class);

    private RuleIndexer _ruleIndexer;

    private final String _zookeeperHosts;
    private final String _rulesPath;

    public DocumentMessageMapper(String zookeeperHosts, String rulesPath) {
        _zookeeperHosts = checkNotNull(zookeeperHosts, "zookeeperHosts");
        _rulesPath = checkNotNull(rulesPath, "rulesPath");
    }

    @Override
    public void init(Config config, TaskContext context) {
        _ruleIndexer = new CuratorRuleIndexer(_zookeeperHosts, _rulesPath, context.getTaskName().getTaskName());
        _ruleIndexer.startAsync().awaitRunning();
    }

    @Override
    public void close() {
        _ruleIndexer.stopAsync().awaitTerminated();
    }

    private boolean doesMatchRule(RuleIndexPair pair, Document doc) {
        Query q = pair.getLevel() == QueryLevel.OUTER ? pair.getRule().getQuery() : pair.getRule().getQuery().getJoin();

        return QueryHelper.meetsFilters(q.getFilters(), doc);
    }

    // determines if a document has the requisite join attribute values to
    // perform the join and fulfill the query. If the query does not have join attributes values, then the
    // method should succeed because we know that the document has the requisite attributes (because they aren't any)
    private boolean documentHasJoinAttributeValues(RuleIndexPair pair, Document doc) {
        if (pair.getRule().getQuery().getJoinAttributes() == null) {
            return true;
        }
        return pair.getRule().getQuery().getJoinAttributes()
                .stream()
                .map(att -> pair.getLevel() == QueryLevel.OUTER ? att.getOuterAttribute() : att.getInnerAttribute())
                .allMatch(att ->  doc.getValue(att) != null);
    }

    private Set<RuleIndexPair> getApplicableQueries(Document document) {

        if (document == null) {
            return ImmutableSet.of();
        }

        return _ruleIndexer.getApplicableRulesForTable(document.getTable())
                .stream()
                .filter(pair -> documentHasJoinAttributeValues(pair, document))
                .filter(pair -> doesMatchRule(pair, document))
                .collect(Collectors.toSet());
    }

    @Override
    public Collection<DocumentMessage> apply(DocumentChangePair changePair) {
        Set<RuleIndexPair> oldApplicableRules = getApplicableQueries(changePair.getOldDocument());
        Set<RuleIndexPair> newApplicableRules = getApplicableQueries(changePair.getNewDocument());

        Set<KeyValue<String, QueryLevel>> newPartitionLocations = newApplicableRules
                .stream()
                .map(pair -> new DefaultKeyValue<>(FanoutHashingUtils.getPartitionKey(changePair.getNewDocument(),
                        pair.getLevel(), pair.getRule().getQuery()), pair.getLevel()))
                .collect(Collectors.toSet());

        Set<RuleIndexPair> rulesForDeletion = oldApplicableRules
                .stream()
                .filter(pair -> !newPartitionLocations.contains(new DefaultKeyValue<>(
                        FanoutHashingUtils.getPartitionKey(changePair.getOldDocument(),
                                pair.getLevel(), pair.getRule().getQuery()), pair.getLevel())))
                .collect(Collectors.toSet());

        Collection<DocumentMessage> messages = new LinkedList<>();

        // add all the update messages
        for (RuleIndexPair rulePair : newApplicableRules) {
            messages.add(new DocumentMessage(changePair.getNewDocument(),
                    FanoutHashingUtils.getPartitionKey(changePair.getNewDocument(),
                            rulePair.getLevel(), rulePair.getRule().getQuery()),
                    rulePair.getLevel(), OpCode.UPDATE, rulePair.getRule().getQuery()));
        }

        // add all the delete messages
        for (RuleIndexPair rulePair : rulesForDeletion) {
            if (!newApplicableRules.contains(rulePair) && rulePair.getLevel() == QueryLevel.OUTER) {
                messages.add(new DocumentMessage(changePair.getOldDocument(),
                        FanoutHashingUtils.getPartitionKey(changePair.getOldDocument(),
                                rulePair.getLevel(), rulePair.getRule().getQuery()),
                        rulePair.getLevel(), OpCode.REMOVE_AND_DELETE, rulePair.getRule().getQuery()));
            } else {
                messages.add(new DocumentMessage(changePair.getOldDocument(),
                        FanoutHashingUtils.getPartitionKey(changePair.getOldDocument(),
                                rulePair.getLevel(), rulePair.getRule().getQuery()),
                        rulePair.getLevel(), OpCode.REMOVE, rulePair.getRule().getQuery()));
            }
        }

        return messages;
    }
}
