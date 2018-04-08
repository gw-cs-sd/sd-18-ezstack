package org.ezstack.denormalizer.bootstrapper;

import com.google.common.base.MoreObjects;
import com.google.common.collect.HashMultimap;
import com.google.common.util.concurrent.AbstractService;
import org.ezstack.denormalizer.model.RuleIndexPair;
import org.ezstack.denormalizer.model.RuleIndexer;
import org.ezstack.ezapp.datastore.api.Rule;

import java.util.Collections;
import java.util.Set;

public class BootstrapperRuleIndexer extends AbstractService implements RuleIndexer {

    private HashMultimap<String, RuleIndexPair> _ruleIndex;
    private final Set<Rule> _rules;

    public BootstrapperRuleIndexer(Set<Rule> rules) {
        _rules = rules;
        _ruleIndex = getRuleIndex(rules);
    }

    @Override
    protected void doStart() {
        notifyStarted();
    }

    @Override
    protected void doStop() {
        notifyStopped();
    }

    @Override
    public Set<Rule> getRules() {
        return _rules;
    }

    @Override
    public Set<RuleIndexPair> getApplicableRulesForTable(String table) {
        return MoreObjects.firstNonNull(_ruleIndex.get(table), Collections.emptySet());
    }
}
