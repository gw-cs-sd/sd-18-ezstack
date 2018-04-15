import org.ezstack.ezapp.datastore.api.Rule;

public class QueryObject {

    private Rule _rule;
    private long _priority;
    private long _recentTimestamp;

    /**
     * This is the object used for storing the incidental value required for creating and maintaining rules. The
     * timestamp is needed to guarantee that the list of rules is as up-to-date as possible, the priority is stored so
     * that the scores do not need to be recalculated every time the RuleDeterminationProcessor executes, and the rule
     * needs to be stored so that there is a rule to be passed in to the ruledeterminationprocessor when it executes.
     * @param priority
     * @param recentTimestamp
     * @param rule
     */
    public QueryObject(long priority, long recentTimestamp, Rule rule) {
        _priority = priority;
        _recentTimestamp = recentTimestamp;
        _rule = rule;
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

    public void setRule(Rule rule) {
        _rule = rule;
    }

    public Rule getRule() {
        return _rule;
    }
}
