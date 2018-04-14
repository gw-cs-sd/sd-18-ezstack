package org.ezstack.ezapp.datastore.api;

public class RuleHelper {
    /**
     * Given an original query it will modify it to a rule query and then build a rule for the modified query.
     * if such a task can't be done then null is returned.
     * @param original
     * @return
     */
    public static Rule getRule(Query original) {
        return new Rule(getRuleQuery(original));
    }

    /**
     * Query - will return the new query that can be safely passed as a rule for the denormalizer.
     * null - will be returned if the original query is not possible to be converted into a rule.
     * @param original
     * @return
     */
    public static Query getRuleQuery(Query original) {
        if (!isTwoLevelQuery(original)) {
            return null;
        }

        return new Query(
                null, // searchTypes
                original.getTable(),
                null, // filters
                original.getJoin(),
                original.getJoinAttributeName(),
                original.getJoinAttributes(),
                original.getExcludeAttributes(),
                original.getIncludeAttributes()
        );
    }

    /**
     * Given the original user query and a query received from a rule it will return a boolean
     * True - indicates they are a close enough match
     * False - not a match, move on.
     * @param original
     * @param rule
     * @return
     */
    public static boolean ruleQueryMatch(Query original, Query rule) {
        if (!isTwoLevelQuery(original) || !isTwoLevelQuery(rule)) {
            return false;
        }

        if (!original.getTable().equals(rule.getTable())) return false;
        if (!original.getFilters().isEmpty() && !original.getFilters().equals(rule.getFilters())) return false;
        if (!original.getJoin().equals(rule.getJoin())) return false;
        if (!original.getJoinAttributes().equals(rule.getJoinAttributes())) return false;
        if (!original.getExcludeAttributes().equals(rule.getExcludeAttributes())) return false;
        if (!original.getIncludeAttributes().equals(rule.getIncludeAttributes())) return false;
        return true;
    }

    /**
     * True - if the query is a two level query
     * False - if query is not a two level query
     * @param q
     * @return
     */
    public static boolean isTwoLevelQuery(Query q) {
        return q.getJoin() != null && q.getJoin().getJoin() == null;
    }

}
