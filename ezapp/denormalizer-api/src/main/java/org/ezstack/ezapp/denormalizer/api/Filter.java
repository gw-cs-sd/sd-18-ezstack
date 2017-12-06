package org.ezstack.ezapp.denormalizer.api;

import java.util.List;
import java.util.Map;

public abstract class Filter implements Denormalization<List<Map<String, Object>>> {

    public Filter(String table, String attribute) {

    }

    @Override
    public void compute() {

    }

    @Override
    public List<Map<String, Object>> getResults() {
        return null;
    }

    public abstract static class Builder {

        Builder() {
        }

    }
}
