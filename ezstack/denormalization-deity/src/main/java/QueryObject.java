import org.ezstack.ezapp.datastore.api.Query;

public class QueryObject {

    private Query _query;
    private long _priority;
    private long _recentTimestamp;

    /**
     * This is the object used for storing the incidental value required for creating and maintaining rules. The
     * timestamp is needed to guarantee that the list of rules is as up-to-date as possible, the priority is stored so
     * that the scores do not need to be recalculated every time the RuleDeterminationProcessor executes, and the query
     * needs to be stored so that there is a reference with which the rules need to be created.
     * @param priority
     * @param recentTimestamp
     * @param query
     */
    public QueryObject(long priority, long recentTimestamp, Query query) {
        _priority = priority;
        _recentTimestamp = recentTimestamp;
        _query = query;
    }

    public long getPriority() {
        return _priority;
    }

    public void setPriority(long priority) {
        _priority = priority;
    }

    public long getRecentTimestamp() {
        return _recentTimestamp;
    }

    public void setRecentTimestamp(long timestamp) {
        _recentTimestamp = timestamp;
    }

    public void setQuery(Query query) {
        _query = _query;
    }

    public Query getQuery() {
        return _query;
    }
}
