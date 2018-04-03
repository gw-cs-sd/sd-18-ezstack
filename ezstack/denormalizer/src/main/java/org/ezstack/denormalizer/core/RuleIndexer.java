package org.ezstack.denormalizer.core;

import com.google.common.util.concurrent.Service;
import org.ezstack.denormalizer.model.RuleIndexPair;
import org.ezstack.ezapp.datastore.api.Rule;

import java.util.Set;

public interface RuleIndexer extends Service {

    Set<Rule> getRules();

    Set<RuleIndexPair> getApplicableRulesForTable(String table);
}
