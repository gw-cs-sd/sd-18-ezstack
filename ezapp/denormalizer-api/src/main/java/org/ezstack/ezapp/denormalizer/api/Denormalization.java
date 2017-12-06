package org.ezstack.ezapp.denormalizer.api;

public interface Denormalization<T> {

    void compute();

    T getResults();
}
