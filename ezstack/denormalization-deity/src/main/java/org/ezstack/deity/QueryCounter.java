package org.ezstack.deity;

import java.util.concurrent.atomic.AtomicInteger;

public class QueryCounter {

    private AtomicInteger _integer;

    /**
     * The purpose of this class is to allow a limited amount of access to a specific AtomicInteger, used for counting
     * the amount of queries that have been passed into the system since the previous execution of the
     * org.ezstack.deity.RuleCreationService.
     */
    public QueryCounter() {
        _integer = new AtomicInteger(0);
    }

    public void increment() {
        _integer.getAndIncrement();
    }

    public int getAndReset() {
        return _integer.getAndSet(0);
    }
}
