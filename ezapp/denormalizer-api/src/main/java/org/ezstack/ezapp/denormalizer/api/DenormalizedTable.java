package org.ezstack.ezapp.denormalizer.api;

import org.ezstack.ezapp.denormalizer.api.AttributeType;
import org.ezstack.ezapp.denormalizer.api.Denormalization;

import java.util.Map;

public class DenormalizedTable {

    private String _sourceTableName;
    private String _denormalizedTableName;
    private Map<String, AttributeType> _mapping;
    private Denormalization[] _denormalizations;
}
