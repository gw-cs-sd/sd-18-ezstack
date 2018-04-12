import org.ezstack.ezapp.datastore.api.Query;

public class QueryObject {

    private Query _query;
    private long _priority;
    private long _recentTimestamp;

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
