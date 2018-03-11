package org.ezstack.ezapp.datastore.api;

import java.util.HashSet;
import java.util.Set;

public class RuleHelper {
    private Query _originalQuery;
    private Rule _closestMatch;
    private RulesManager _manager;

    public RuleHelper(Query originalQuery, RulesManager manager) {
        _originalQuery = originalQuery;
        _manager =manager;

        computeClosestMatch();
    }

    /**
     * computes the closest matching rule if any exist.
     * It is made public in case a developer wants to do periodic checks on if more efficient
     * rules relating to the query have been created. However, the class itself only does that
     * compilation once.
     */
    public void computeClosestMatch() {
        if (!isTwoLevelQuery(_originalQuery)) {
            return;
        }

        double highestMatch = 0;
        Rule closestRuleMatch = null;

        String outerTable = _originalQuery.getTable();
        String innerTable = _originalQuery.getJoin().getTable();
        for (Rule r: _manager.getRules(outerTable, innerTable, Rule.RuleStatus.ACTIVE)) {
            if (queryCloseness(_originalQuery, r.getQuery()) > highestMatch) {
                closestRuleMatch = r;
            }
        }

        _closestMatch = closestRuleMatch;
    }

    /**
     * The higher the number the closer these two queries match.
     * @param q1
     * @param q2
     * @return 0 if can't do conversion from q1 to q2, or a positive number if it can be done.
     */
    public static double queryCloseness(Query q1, Query q2) {
        // TODO
        return 0;
    }

    /**
     * Its considered simple because it does not test inner queries.
     * @param q1
     * @param q2
     * @return
     */
    public static boolean simpleQueriesCanBeClose(Query q1, Query q2) {
        if (!q1.getTable().equals(q2.getTable())) return false;
        if (QueryHelper.userWantsDocuments(q1.getSearchTypes()) != QueryHelper.userWantsDocuments(q2.getSearchTypes()))
            return false;
        if (!setEncopassesSet(q1.getExcludeAttributes(), q2.getExcludeAttributes())) return false;
        if (!setEncopassesSet(q2.getIncludeAttributes(), q1.getIncludeAttributes())) return false;
        if (!setEncopassesSet(q1.getFilters(), q2.getFilters())) return false;
        if (!setEncopassesSet(q1.getJoinAttributes(), q2.getJoinAttributes())) return false;

        return true;
    }

    public static <T> boolean setEncopassesSet(Set<T> largerSet, Set<T> smallerSet) {
        for (T t: smallerSet) {
            if (!largerSet.contains(t)) {
                return false;
            }
        }

        return true;
    }

    /**
     * @param query
     * @return new rule compliant query, or null if original query could not be converted
     */
    public static Query getRuleCompliantQuery(Query query) {
        // For now all queries turned into rules must be at least 2 levels long
        if (!isTwoLevelQuery(query)) {
            return null;
        }

        Set<SearchType> searchTypes = new HashSet<>();
        if (QueryHelper.userWantsDocuments(query.getSearchTypes())) {
            searchTypes.add(new SearchType(SearchType.Type.SEARCH.toString(), null));
        }

        return new Query(searchTypes,
                query.getTable(),
                query.getFilters(),
                query.getJoin(),
                query.getJoinAttributeName(),
                query.getJoinAttributes(),
                query.getExcludeAttributes(),
                query.getIncludeAttributes());
    }

    private static boolean isTwoLevelQuery(Query q) {
        return q.getJoin() != null && q.getJoin().getJoin() == null;
    }
}
