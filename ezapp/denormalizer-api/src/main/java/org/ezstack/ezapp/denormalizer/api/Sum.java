package org.ezstack.ezapp.denormalizer.api;

public abstract class Sum<T extends Number> extends Aggregation<T> {

    @Override
    public T getResults() {
        return null;
    }
}
