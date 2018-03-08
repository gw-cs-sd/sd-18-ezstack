package org.ezstack.ezapp.datastore.api;

import java.util.Set;

public interface RulesManager {

    void create(Rule rule) throws RuleAlreadyExistsException;

    void remove(Rule rule);

    Set<Rule> getRules();

    Set<Rule> getActiveRules(String outerTable);

    Set<Rule> getActiveRules(String outerTable, String innerTable);

}
