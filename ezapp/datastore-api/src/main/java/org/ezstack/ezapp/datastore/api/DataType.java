package org.ezstack.ezapp.datastore.api;

import java.util.Collection;
import java.util.Map;

public class DataType {
    enum JsonTypes {
        INTEGER, DOUBLE, BOOLEAN, STRING, LIST, MAP, UNKNOWN
    }

    public static JsonTypes getDataType(Object obj) {
        if (obj instanceof Integer) {
            return JsonTypes.INTEGER;
        } else if (obj instanceof Double) {
            return JsonTypes.DOUBLE;
        } else if (obj instanceof Boolean) {
            return JsonTypes.BOOLEAN;
        } else if (obj instanceof String) {
            return JsonTypes.STRING;
        } else if (obj instanceof Collection<?>) {
            return JsonTypes.LIST;
        } else if (obj instanceof Map<?, ?>) {
            return JsonTypes.MAP;
        } else {
            return JsonTypes.UNKNOWN;
        }
    }
}
