package org.ezstack.ezapp.datastore.api;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class DataTypeTest {

    private final String jsonObject = "{\"array\": [1,2,3],\"boolean\": true,\"null\": null,\"integer\": 1,\"double\" : 2.5,\"object\": {\"a\": \"b\",\"c\": \"d\",\"e\": \"f\"},\"string\": \"Hello World\"}";
    private Map<String, Object> json;
    private ObjectMapper mapper;

    @Before
    public void buildJson() throws IOException {
        mapper = new ObjectMapper();

        try {
            json = mapper.readValue(jsonObject, new TypeReference<Map<String, Object>>() {
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testDataTypeList() {
        assertEquals(DataType.getDataType(json.get("array")), DataType.JsonTypes.LIST);
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
