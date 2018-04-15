package org.ezstack.ezapp.datastore.api;

import java.util.HashSet;
import java.util.Set;

public class RuleExecutor {
    private RulesManager _ruleManager;
    private Query _originalQuery;
    private Query _execQuery;
    private Rule _closestRule;

    public RuleExecutor(Query originalQuery, RulesManager ruleManager) {
        _originalQuery = originalQuery;
        _ruleManager = ruleManager;

        _execQuery = null;
        _closestRule = null;

        computeClosestRule();
    }

    /**
     * method already gets called by constructor to get the closest rule,
     * however it has been made public in case a user wants to check if a
     * rule for this query has been created since object creation.
     */
    public void computeClosestRule() {
        if (_closestRule != null || !RuleHelper.isTwoLevelQuery(_originalQuery)) {
            return;
        }

        String outerTable = _originalQuery.getTable();
        String innerTable = _originalQuery.getJoin().getTable();

        for (Rule r : _ruleManager.getRules(outerTable, innerTable, Rule.RuleStatus.ACTIVE)) {
            if (RuleHelper.ruleQueryMatch(_originalQuery, r.getQuery())) {
                _closestRule = r;
                computeExecQuery();
                return;
            }
        }
    }

    private void computeExecQuery() {
        Set<Filter> filters = _originalQuery.getFilters();
        if (filters.equals(_closestRule.getQuery().getFilters())) {
            // if the filters are the same then ES doesn't need to do anything
            // because the filters have been computed by the denormalizer :)
            filters = new HashSet<>();
        }

        _execQuery = new Query(_originalQuery.getSearchTypes(),
                _closestRule.getTable(),
                filters,
                null,
                null,
                null,
                null,
                null
                );
    }

    public Query getExecQuery() {
        if (_execQuery == null) {
            return _originalQuery;
        }

        return _execQuery;
    }

    /**
     * Will check if RuleExecutor is using a modified query from the original query requested.
     * If it is then it will modify the document to add/remove the needed data from the original
     * query. Otherwise if it detects that the original query was used then it will return the
     * document as is umodified.
     * @param doc
     * @return
     */
    public Document correctfyDocument(Document doc) {
        // exec occured on original query so no modifications
        if (_execQuery == null) {
            return doc;
        }

        String swapKey = _closestRule.getQuery().getJoinAttributeName();
        String newKey = _originalQuery.getJoinAttributeName();

        if (!swapKey.equals(newKey)) {
            Object data = doc.getValue(swapKey);
            doc.remove(swapKey);
            doc.setDataField(newKey, data);
        }

        return doc;
    }
}
