package org.ezstack.aggregations;

public class Average implements QueryVerb {

    private String field;

    public String getVerbName() {
        return "Average";
    }

    public void setFieldName(String fieldName) {
        field = fieldName;
    }

    public String getFieldName() {
        return field;
    }
}
