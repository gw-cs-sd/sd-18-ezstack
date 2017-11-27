package org.ezstack.aggregations;

import static com.google.common.base.Preconditions.checkArgument;

public class Query {

    private long responseTimeNs;
    private QueryVerb[] verbs;
    private String index;
    private TempRule[] rules;

    public Query(QueryVerb[] verbList, String queryIndex) {
        verbs = verbList;
        index = queryIndex;
        setRules();
        responseTimeNs = 0;
    }

    private void setRules() {
        rules = new TempRule[verbs.length];
        for (int i = 0; i < verbs.length; i++) {
            rules[i] = new TempRule(index, verbs[i]);
        }
    }

    public void setResponseTime(long time) {
        checkArgument(time > 0, "Response time is less than zero");
        responseTimeNs = time;
    }

    public long getResponseTime() {
        if (responseTimeNs != 0)
            return responseTimeNs;
        System.out.println("response time not found");
        return 0;
    }

    public TempRule[] getRules() {
        return rules;
    }

    public QueryVerb[] getVerbs() {
        return verbs;
    }

    public String getIndex() {
        return index;
    }

}
