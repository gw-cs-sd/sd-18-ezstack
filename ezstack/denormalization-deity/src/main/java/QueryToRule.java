import org.ezstack.ezapp.datastore.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.function.Supplier;

public class QueryToRule {

    private final RulesManager _rulesManager;
    private final Supplier<Set<Rule>> _rules;
    private static final Logger LOG = LoggerFactory.getLogger(RuleDeterminationProcessor.class);

    public QueryToRule(RulesManager rulesManager, Supplier<Set<Rule>> rules) {
        _rulesManager = rulesManager;
        _rules = rules;
    }

    /**
     * This function is what is used to generate the rule that is sent to the denormalizer. The necessary components of
     * the function are to make sure that the rule we get is not null, as well as making sure the rule does not already
     * exist within the denormalizer.
     * @param query
     * @return
     */
    public Rule convertToRule(Query query, int maxRules) {

        RuleHelper helper = new RuleHelper();
        Rule rule = helper.getRule(query);

        if (rule != null) {
            if (_rules.get().size() <= maxRules) {
                if (!ruleExists(rule)) {
                    return rule;
                }
            }
            else {
                LOG.info("Maximum Rules Hit");
                return null;
            }
        }
        return null;
    }

    /**
     * We need to try to make sure that the rules we try to create do not already exist, because if we attempt to call
     * the add function on a rule that already exists, it will throw an exception that we would then have to catch.
     * @param rule
     * @return
     */
    public boolean ruleExists(Rule rule) {
        return _rules.get().contains(rule);
    }

    /**
     * Here, we add the rule, and if the rule already exists, we need to catch the RuleAlreadyExistsException.
     * @param rule
     */
    public void addRule(Rule rule) {
        try {
            _rulesManager.createRule(rule);
        } catch (RuleAlreadyExistsException exception) {
            return;
        }
    }

}
