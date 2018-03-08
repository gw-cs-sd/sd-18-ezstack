package org.ezstack.ezapp.datastore.api;

import java.util.Set;

import static org.ezstack.ezapp.datastore.api.Rule.RuleStatus;

public interface RulesManager {

    void create(Rule rule) throws RuleAlreadyExistsException;

    void remove(Rule rule);

    Set<Rule> getRules();

    Set<Rule> getRules(RuleStatus status);

    Set<Rule> getRules(String outerTable, RuleStatus status);

    Set<Rule> getRules(String outerTable, String innerTable, RuleStatus status);

}
