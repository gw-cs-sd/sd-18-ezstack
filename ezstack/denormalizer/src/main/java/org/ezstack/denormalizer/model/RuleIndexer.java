package org.ezstack.denormalizer.model;

import com.google.common.collect.HashMultimap;
import com.google.common.util.concurrent.Service;
import org.ezstack.denormalizer.model.RuleIndexPair;
import org.ezstack.ezapp.datastore.api.Rule;

import java.util.Collection;
import java.util.Set;

public interface RuleIndexer extends Service {

    Set<Rule> getRules();

    Set<RuleIndexPair> getApplicableRulesForTable(String table);

    default HashMultimap<String, RuleIndexPair> getRuleIndex(Collection<Rule> rules) {
        HashMultimap<String, RuleIndexPair> index = HashMultimap.create();

        for (Rule rule : rules) {
            index.put(rule.getQuery().getTable(), new RuleIndexPair(rule, QueryLevel.OUTER));

            if (rule.getQuery().getJoin() != null) {
                index.put(rule.getQuery().getJoin().getTable(), new RuleIndexPair(rule, QueryLevel.INNER));
            }
        }

        return index;
    }
}
