package org.ezstack.aggregations;

public class Max implements QueryVerb {

    private String field;

    public String getVerbName() {
        return "Max";
    }

    public void setFieldName(String fieldName) {
        field = fieldName;
    }

    public String getFieldName() {
        return field;
    }
}
