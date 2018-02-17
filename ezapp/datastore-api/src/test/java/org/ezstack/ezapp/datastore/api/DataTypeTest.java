package org.ezstack.ezapp.datastore.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class DataTypeTest {

    private final String jsonObject = "{\"array\": [1, \"Hello\", true], \"boolean\": true, \"null\": null, " +
            "\"integer\": 1, \"long\": 9223372036854775806, \"double\" : 2.5, " +
            "\"object\": {\"a\": \"b\", \"c\": \"d\", \"e\": \"f\"}, \"string\": \"Hello World\"}";
    private Map<String, Object> json;
    private ObjectMapper mapper;

    @Before
    public void buildJson() throws IOException {
        mapper = new ObjectMapper();

        try {
            json = mapper.readValue(jsonObject, Map.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testDataTypeList() {
        assertEquals(DataType.getDataType(json.get("array")), DataType.JsonTypes.LIST);
    }

    @Test
    public void testDataTypeListValues() {
        List list = (List) json.get("array");
        assertEquals(DataType.getDataType(list.get(0)), DataType.JsonTypes.INTEGER);
        assertEquals(DataType.getDataType(list.get(1)), DataType.JsonTypes.STRING);
        assertEquals(DataType.getDataType(list.get(2)), DataType.JsonTypes.BOOLEAN);
    }

    @Test
    public void testDataTypeBoolean() {
        assertEquals(DataType.getDataType(json.get("boolean")), DataType.JsonTypes.BOOLEAN);
    }

    @Test
    public void testDataTypeNull() {
        assertEquals(DataType.getDataType(json.get("null")), DataType.JsonTypes.UNKNOWN);
    }

    @Test
    public void testDataTypeInteger() {
        assertEquals(DataType.getDataType(json.get("integer")), DataType.JsonTypes.INTEGER);
    }

    @Test
    public void testDataTypeLong() {
        assertEquals(DataType.getDataType(json.get("long")), DataType.JsonTypes.LONG);
    }

    @Test
    public void testDataTypeDouble() {
        assertEquals(DataType.getDataType(json.get("double")), DataType.JsonTypes.DOUBLE);
    }

    @Test
    public void testDataTypeMap() {
        assertEquals(DataType.getDataType(json.get("object")), DataType.JsonTypes.MAP);
    }

    @Test
    public void testDataTypeString() {
        assertEquals(DataType.getDataType(json.get("string")), DataType.JsonTypes.STRING);
    }
}
