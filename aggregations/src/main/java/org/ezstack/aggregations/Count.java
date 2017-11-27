package org.ezstack.aggregations;

public class Count implements QueryVerb {

    private String field;

    public String getVerbName() {
        return "Count";
    }

    public void setFieldName(String fieldName) {
        field = fieldName;
    }

    public String getFieldName() {
        return field;
    }
}
