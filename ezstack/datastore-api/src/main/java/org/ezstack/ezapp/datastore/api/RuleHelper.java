package org.ezstack.ezapp.datastore.api;

import java.util.HashSet;
import java.util.Set;

public class RuleHelper {
    private Query _originalQuery;
    private Query _execQuery;
    private QueryWeight _weight;
    private RulesManager _manager;
    private Rule _closestMatch;
    private double _closestMatchScore;

    public RuleHelper(Query originalQuery, RulesManager manager) {
        this(originalQuery, new QueryWeight(), manager);
    }

    public RuleHelper(Query originalQuery, QueryWeight weight, RulesManager manager) {
        _originalQuery = originalQuery;
        _manager =manager;
        _weight = weight;

        _execQuery = null;
        _closestMatch = null;
        _closestMatchScore = 0;
        computeClosestMatch();
    }

    private void computeClosestMatch() {
        if (!isTwoLevelQuery(_originalQuery) || _closestMatchScore == _weight.getTotal()) {
            return;
        }

        double highestMatch = 0;
        Rule closestRuleMatch = null;

        String outerTable = _originalQuery.getTable();
        String innerTable = _originalQuery.getJoin().getTable();
        for (Rule r: _manager.getRules(outerTable, innerTable, Rule.RuleStatus.ACTIVE)) {
            double temp = queryCloseness(_originalQuery, r.getQuery(), _weight);
            if (temp > highestMatch) {
                highestMatch = temp;
                closestRuleMatch = r;
            }
        }

        _closestMatch = closestRuleMatch;
        _closestMatchScore = highestMatch;
        computeExecQuery();
    }

    private void computeExecQuery() {
        if (_closestMatch == null) {
            return;
        }

        // Top level query modifications only
        // all other modifications have to be done after document is retrieved from ES
        Set<SearchType> searchTypes = asymmetricSet(_originalQuery.getSearchTypes(), _closestMatch.getQuery().getSearchTypes());
        String tableName = _closestMatch.getTable();
        Set<Filter> filters = asymmetricSet(_originalQuery.getFilters(), _closestMatch.getQuery().getFilters());
        Set<String> excludeAttributes = asymmetricSet(_originalQuery.getExcludeAttributes(),
                _closestMatch.getQuery().getExcludeAttributes());
        Set<String> includeAttributes = _originalQuery.getIncludeAttributes();

        _execQuery = new Query(searchTypes, tableName, filters, null,
                null, null, excludeAttributes, includeAttributes);
    }

    public Query getExecQuery() {
        if (_execQuery == null) {
            return _originalQuery;
        }

        return _execQuery;
    }

    /**
     * The higher the number the closer these two queries match.
     * @param q1
     * @param q2
     * @return 0 if can't do conversion from q1 to q2, or a positive number if it can be done.
     */
    public static double queryCloseness(Query q1, Query q2, QueryWeight weight) {
        if (!simpleQueriesCanBeClose(q1, q2)) {
            return 0;
        }

        double join = weight.getJoinQuery();
        if (q1.getJoin() != null && q2.getJoin() != null) {
            double rawJoin = queryCloseness(q1.getJoin(), q2.getJoin(), weight);
            if (rawJoin == 0) {
                return 0;
            }
            join = (rawJoin/weight.getTotal()) * weight.getJoinQuery();
        } else if (!(q1.getJoin() == null && q2.getJoin() == null)) {
            return 0;
        }

        double searchTypes = computeWeight(q1.getSearchTypes(), q2.getSearchTypes(), weight.getSearchTypes());
        double tableName = weight.getTableName();
        double filters = computeWeight(q1.getFilters(), q2.getFilters(), weight.getFilters());
        double joinAttributeName = q1.getJoinAttributeName().equals(q2.getJoinAttributeName()) ?
                weight.getJoinAttributeName() : 0;
        double joinAttributes = computeWeight(q1.getJoinAttributes(), q2.getJoinAttributes(), weight.getJoinAttributes());
        double excludeAttributes = computeWeight(q1.getExcludeAttributes(), q2.getExcludeAttributes(), weight.getExcludeAttributes());
        double includeAttributes = q2.getIncludeAttributes().isEmpty() ?
                computeWeight(q1.getIncludeAttributes(), q2.getIncludeAttributes(), weight.getIncludeAttributes())
                : computeWeight(q2.getIncludeAttributes(), q1.getIncludeAttributes(), weight.getIncludeAttributes());

        return searchTypes + tableName + filters + join + joinAttributeName +
                joinAttributes + excludeAttributes + includeAttributes;
    }

    public static <T> double computeWeight(Set<T> s1, Set<T> s2, int maxWeight) {
        if (s1.size() == 0) {
            return maxWeight;
        }

        Set<T> s3 = asymmetricSet(s1, s2);
        int diff = s1.size() - s3.size();
        return ((double)diff/s1.size()) * maxWeight;
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
        if (!q2.getIncludeAttributes().isEmpty() &&
                !setEncopassesSet(q2.getIncludeAttributes(), q1.getIncludeAttributes())) return false;
        if (!setEncopassesSet(q1.getFilters(), q2.getFilters())) return false;
        if (!setEncopassesSet(q1.getJoinAttributes(), q2.getJoinAttributes())) return false;

        return true;
    }

    /**
     * @param q1
     * @param q2
     * @param <T>
     * @return a new set with all the elements in q1 that are not in q2. (Does not modify inputs)
     */
    public static <T> Set<T> asymmetricSet(Set<T> q1, Set<T> q2) {
        Set<T> ret = new HashSet<>(q1);
        ret.removeAll(q2);
        return ret;
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
        // For now all queries turned into rules must be exactly 2 levels long
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
