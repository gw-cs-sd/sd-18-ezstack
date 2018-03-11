package org.ezstack.ezapp.datastore.api;

import java.util.HashSet;
import java.util.Set;

public class RuleHelper {
    /**
     * @param query
     * @return new rule compliant query, or null if original query could not be converted
     */
    public static Query getRuleCompliantQuery(Query query) {
        // For now all queries turned into rules must be at least 2 levels long
        if (query.getJoin() == null || query.getJoin().getJoin() != null) {
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
}
