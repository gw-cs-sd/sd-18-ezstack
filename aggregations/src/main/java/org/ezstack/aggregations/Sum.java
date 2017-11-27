package org.ezstack.aggregations;

public class Sum implements QueryVerb {

    private String field;

    public String getVerbName() {
        return "Sum";
    }

    public void setFieldName(String fieldName) {
        field = fieldName;
    }

    public String getFieldName() {
        return field;
    }
}
