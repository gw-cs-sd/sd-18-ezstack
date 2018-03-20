public class QueryObject {

    private final String _queryKey;
    private long _priority;
    private long _recentTimestamp;

    public QueryObject(String queryKey, long priority, long recentTimestamp) {
        _queryKey = queryKey;
        _priority = priority;
        _recentTimestamp = recentTimestamp;
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
}
