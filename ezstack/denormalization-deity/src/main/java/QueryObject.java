import org.ezstack.ezapp.datastore.api.Query;

public class QueryObject {

    private final String _queryKey;
    private Query _query;
    private long _priority;
    private long _recentTimestamp;

    public QueryObject(String queryKey, long priority, long recentTimestamp, Query query) {
        _queryKey = queryKey;
        _priority = priority;
        _recentTimestamp = recentTimestamp;
        _query = query;
    }

    public long getPriority() {
        return _priority;
    }

    public String getQueryKey() {
        return _queryKey;
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
