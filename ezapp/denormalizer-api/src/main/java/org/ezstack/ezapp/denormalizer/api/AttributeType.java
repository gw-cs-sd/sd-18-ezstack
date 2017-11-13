package org.ezstack.ezapp.denormalizer.api;

import java.util.Date;

public enum AttributeType {
    STRING(String.class),
    INTEGER(Integer.class),
    DOUBLE(Double.class),
    BOOLEAN(Boolean.class),
    OBJECT(Object.class),
    DATE(Date.class),
    POINTER(Pointer.class);

    private final Object _type;

    AttributeType(Object type) {
        _type = type;
    }

    public Object getType() {
        return _type;
    }
}
