package org.ezstack.aggregations;

import java.util.ArrayList;
import java.util.Collections;

public class Aggregation {

    private Query query;
    private int count;
    private Percentiles percentiles;
    private ArrayList<Long> sortedResponseTimes = new ArrayList<Long>();

    public Aggregation(Query query) {
        this.query = query;
        count = 1;
        sortedResponseTimes.add(query.getResponseTime());
        percentiles = new Percentiles(query.getResponseTime(), query.getResponseTime(), query.getResponseTime(), query.getResponseTime(), query.getResponseTime());

    }

    public void Add(long responseTime) {
        sortedResponseTimes.add(responseTime);
        Collections.sort(sortedResponseTimes);
        generatePercentiles(responseTime);
    }

    public void generatePercentiles(long responseTime) {
        setAvg(responseTime);
        percentiles.setFiftieth(sortedResponseTimes.get(count/2));
        percentiles.setNinetieth(sortedResponseTimes.get(9*(count/10)));
        percentiles.setNinetyFifth(sortedResponseTimes.get(19*(count/20)));
        percentiles.setNinetyNinth(sortedResponseTimes.get(99*(count/100)));

    }

    public void setAvg(long responseTime) {
        long avg = (count*(percentiles.getAvg())) + responseTime;
        count++;
        percentiles.setAvg(avg/count);
    }
}
