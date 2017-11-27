package org.ezstack.aggregations;

public class App
{
    void createQuery(String index, QueryVerb[] verbs) {

        Query newQuery = new Query(verbs, index);

    }
}