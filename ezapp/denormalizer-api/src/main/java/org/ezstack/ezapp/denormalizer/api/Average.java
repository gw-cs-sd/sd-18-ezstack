package org.ezstack.ezapp.denormalizer.api;

import java.util.Iterator;
import java.util.Map;

public class Average implements Aggregation<Double> {

    private final AttributeType[] ACCEPTABLE_TYPES = { AttributeType.INTEGER, AttributeType.DOUBLE };

    private final String _attributeToAvg;

    private final Iterable<Map<String, Object>> _documentsToAvg;

    public Average(String attributeToAvg, Iterable<Map<String, Object>> documentsToAvg) {
        _attributeToAvg = attributeToAvg;
        _documentsToAvg = documentsToAvg;
    }

    public Double compute() {
        double sum = 0;
        for (Map<String, Object> doc : _documentsToAvg) {
            if (doc.get(_attributeToAvg) != null)
        }
    }
}
