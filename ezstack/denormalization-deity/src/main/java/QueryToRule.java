import org.ezstack.ezapp.client.EZappClientFactory;
import org.ezstack.ezapp.datastore.api.*;
import java.util.Set;

public class QueryToRule {

    private DeityConfig _config;

    public Rule convertToRule(Query query, DeityConfig config) {
        _config = config;
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
        DeityConfig deityConfig = new DeityConfig(_config);
        RulesManager rulesManager = EZappClientFactory.newRulesManager(deityConfig.getUriAddress());
        Set<Rule> ruleSet = rulesManager.getRules();

        for (Rule ruleInSet:ruleSet) {
            if (ruleInSet == rule) {
                return false;
            }
        }

        return true;
    }

    public void addRule(Rule rule) {
        DeityConfig deityConfig = new DeityConfig(_config);
        RulesManager rulesManager = EZappClientFactory.newRulesManager(deityConfig.getUriAddress());
        try {
            rulesManager.createRule(rule);
        } catch (RuleAlreadyExistsException exception) {
            return;
        }
    }

}
