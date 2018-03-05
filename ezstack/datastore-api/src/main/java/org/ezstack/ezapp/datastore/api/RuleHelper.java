package org.ezstack.ezapp.datastore.api;

import java.util.LinkedList;
import java.util.List;

public class RuleHelper {

    /**
     * @param query
     * @return a modified query that can be made into a rule or null if query doesn't qualify
     */
    public static Query generateRuleCompliantQuery(Query query) {
        // Queries must be at least 2 levels long
        if (query.getJoin() == null || query.getJoin().getJoin() != null) {
            return null;
        }

        Query innerQ = query.getJoin();

        List<SearchType> searchTypes = null;
        if (QueryHelper.hasSearchRequest(query.getSearchTypes())) {
            searchTypes = new LinkedList<>();
            searchTypes.add(new SearchType(SearchType.Type.SEARCH.toString(), null));
        }

        // TODO
        return null;
    }
}
