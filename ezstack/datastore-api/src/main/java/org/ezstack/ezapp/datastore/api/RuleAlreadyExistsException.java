package org.ezstack.ezapp.datastore.api;

public class RuleAlreadyExistsException extends Exception {
    public RuleAlreadyExistsException(String tableName) {
        super("A rule already exists for denormalized table: " + tableName);
    }
}
