package org.ezstack.ezapp.denormalizer.api;

import java.util.Map;

public class Document {

    private final Map<String, Object> _source;
    private final Map<String, AttributeType> _mapping;

    public Document(Map<String, Object> source, Map<String, AttributeType> mapping) {
        _source = source;
        _mapping = mapping;
    }

    public Double getValue(String key) {
        if (_mapping.get(key) == AttributeType.DOUBLE) {

        }
        return 0.;
    }
}
