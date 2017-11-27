package org.ezstack.aggregations;

public class Join implements QueryVerb {

    private String index;

    public String getVerbName() {
        return "Join";
    }

    public void setFieldName(String fieldName) {
        index = fieldName;
    }

    public String getFieldName() {
        return index;
    }
}
