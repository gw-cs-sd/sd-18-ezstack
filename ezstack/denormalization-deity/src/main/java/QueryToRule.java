import org.ezstack.ezapp.datastore.api.*;
import java.util.Set;
import java.util.function.Supplier;

public class QueryToRule {

    private final RulesManager _rulesManager;
    private final Supplier<Set<Rule>> _rules;

    public QueryToRule(RulesManager rulesManager, Supplier<Set<Rule>> rules) {
        _rulesManager = rulesManager;
        _rules = rules;
    }

    public Rule convertToRule(Query query) {

        RuleHelper helper = new RuleHelper();
        Rule rule = helper.getRule(query);

        if (rule != null) {
            if (!ruleExists(rule)) {
                return rule;
            }
        }

        return null;
    }

    public boolean ruleExists(Rule rule) {
        return _rules.get().contains(rule);
    }

    public void addRule(Rule rule) {
        try {
            _rulesManager.createRule(rule);
        } catch (RuleAlreadyExistsException exception) {
            return;
        }
    }

}
