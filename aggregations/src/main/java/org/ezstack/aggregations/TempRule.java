package org.ezstack.aggregations;

class TempRule { //we havent defined a way of laying out how we "display" rules

    private String index;
    private String[] verbFieldPairing = new String[2];

    public TempRule(String indexA, QueryVerb verb) {
        index = indexA;
        verbFieldPairing[0] = verb.getVerbName();
        verbFieldPairing[1] = verb.getFieldName();
    }
}