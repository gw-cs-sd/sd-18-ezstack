package org.ezstack.ezapp.datastore.api;

import java.util.Set;

import static org.ezstack.ezapp.datastore.api.Rule.RuleStatus;

public interface RulesManager {

    void createRule(Rule rule) throws RuleAlreadyExistsException;

    void removeRule(String ruleTable);

    void setRuleStatus(String ruleTable, RuleStatus status);

    Rule getRule(String ruleTable);

    Set<Rule> getRules();

    Set<Rule> getRules(RuleStatus status);

    Set<Rule> getRules(String outerTable, RuleStatus status);

    Set<Rule> getRules(String outerTable, String innerTable, RuleStatus status);

}
