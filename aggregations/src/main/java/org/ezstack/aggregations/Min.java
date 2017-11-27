package org.ezstack.aggregations;

public class Min implements QueryVerb {

    private String field;

    public String getVerbName() {
        return "Min";
    }

    public void setFieldName(String fieldName) {
        field = fieldName;
    }

    public String getFieldName() {
        return field;
    }
}
