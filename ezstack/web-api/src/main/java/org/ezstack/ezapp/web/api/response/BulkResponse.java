package org.ezstack.ezapp.web.api.response;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.LinkedList;
import java.util.HashMap;
import java.util.List;

public class BulkResponse {
    @JsonProperty("errors")
    private long _errors;

    @JsonProperty("items")
    private List<Object>  _items;

    @JsonProperty("errorMessages")
    private List<Object> _errorMessages;

    public BulkResponse() {
        _errors = 0;
        _items = new LinkedList<>();
        _errorMessages = new LinkedList<>();
    }

    public BulkResponse(long errors, List<Object> items, List<Object> errorMessages) {
        _errors = errors;
        _items = items;
        _errorMessages = errorMessages;
    }

    public void addToErrorCount() {
        _errors++;
    }

    public void addItem(Object o) {
        _items.add(o);
    }

    public void addErrorMessage(Object o) {
        _errorMessages.add(o);
    }

    public long getErrors() {
        return _errors;
    }

    public void setErrors(long errors) {
        _errors = errors;
    }

    public List<Object> getItems() {
        return _items;
    }

    public void setItems(List<Object> items) {
        _items = items;
    }

    public List<Object> getErrorMessages() {
        return _errorMessages;
    }

    public void setErrorMessages(List<Object> errorMessages) {
        _errorMessages = errorMessages;
    }

    public static HashMap<String, Object> createGenericErrorMessage(String errorMessage, Object document) {
        HashMap<String, Object> error = new HashMap<>();
        error.put("errorMessage", errorMessage);
        error.put("document", document);
        return error;
    }
}
