import org.ezstack.ezapp.datastore.api.*;

import java.util.Set;

public class QueryToRule {

    public Rule convertToRule(Query query) {

        RuleHelper helper = new RuleHelper();
        Rule rule = helper.getRule(query);

        if (rule != null) {
            if (ruleExists(rule)) {
                return rule;
            }
        }

        return null;
    }

    public boolean ruleExists(Rule rule) {
        RulesManager rulesManager = new RulesManager();
        Set<Rule> ruleSet = rulesManager.getRules();

        for (Rule ruleInSet:ruleSet) {
            if (ruleInSet == rule) {
                return false;
            }
        }

        return true;
    }

    public void addRule(Rule rule) {
        RulesManager rulesManager = new RulesManager();
        try {
            rulesManager.create(rule);
        } catch (RuleAlreadyExistsException exception) {
            return;
        }
    }

}
