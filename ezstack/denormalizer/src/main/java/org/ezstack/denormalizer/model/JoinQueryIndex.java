package org.ezstack.denormalizer.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import org.ezstack.ezapp.datastore.api.Document;

import java.util.*;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;

public class JoinQueryIndex {

    @JsonProperty("outerDocs")
    private final Map<String, Document> _outerDocs;

    @JsonProperty("innnerDocs")
    private final Map<String, Document> _innerDocs;

    private Document _modifiedDocument;
    private QueryLevel _modifiedLevel;
    private boolean _isModified;

    public JoinQueryIndex() {
        _outerDocs = new LinkedHashMap<>();
        _innerDocs = new LinkedHashMap<>();
        _isModified = false;

    }

    @JsonCreator
    private JoinQueryIndex(@JsonProperty("outerDocs") Map<String, Document> outerDocs,
                           @JsonProperty("innerDocs") Map<String, Document> innerDocs) {
        _outerDocs = outerDocs;
        _innerDocs = innerDocs;
        _isModified = false;

    }

    public void putDocument(Document document, QueryLevel queryLevel) {

        checkArgument(!_isModified, "Query Index can only be modified once");

        _modifiedDocument = document;
        _modifiedLevel = queryLevel;
        _isModified = true;

        if (queryLevel == QueryLevel.OUTER) {
            _outerDocs.put(document.getKey(), document);
        } else {
            _innerDocs.put(document.getKey(), document);
        }
    }

    public void deleteDocument(Document document, QueryLevel queryLevel) {
        checkArgument(!_isModified, "Query Index can only be modified once");

        _modifiedDocument = document;
        _modifiedLevel = queryLevel;
        _isModified = true;

        if (queryLevel == QueryLevel.OUTER) {
            _outerDocs.remove(document.getKey());
        } else {
            _innerDocs.remove(document.getKey());
        }
    }

    @JsonIgnore
    public List<Document> getEffectedDocumentsOuter() {
        if (!_isModified) {
            return Collections.emptyList();
        }

        if (_modifiedLevel == QueryLevel.OUTER) {
            return ImmutableList.of(_modifiedDocument);
        }

        return _outerDocs.entrySet().stream().map(Map.Entry::getValue).collect(Collectors.toList());
    }

    @JsonIgnore
    public List<Document> getEffectedDocumentsInner() {
        if (!_isModified) {
            return Collections.emptyList();
        }

        return _innerDocs.entrySet().stream().map(Map.Entry::getValue).collect(Collectors.toList());
    }

    // This is necessary because samza has a deserialized cache, which means that a we need to clear out metadata
    // after each use
    public void refresh() {
        _isModified = false;
        _modifiedLevel = null;
        _modifiedDocument = null;
    }
}
