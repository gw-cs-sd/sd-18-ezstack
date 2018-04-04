package org.ezstack.denormalizer.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import org.ezstack.ezapp.datastore.api.Document;

import java.util.*;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

import static com.google.common.base.Preconditions.checkArgument;

public class JoinQueryIndex {

    @JsonProperty("outerDocs")
    private final Map<String, Document> _outerDocs;

    @JsonProperty("innerDocs")
    private final Map<String, Document> _innerDocs;

    @JsonProperty("innerTombstones")
    private final Map<String, Set<Integer>> _innerTombstones;

    @JsonProperty("outerTombstones")
    private final Map<String, Set<Integer>> _outerTombstones;

    private Document _modifiedDocument;
    private QueryLevel _modifiedLevel;
    private boolean _isModified;

    public JoinQueryIndex() {
        _outerDocs = new LinkedHashMap<>();
        _innerDocs = new LinkedHashMap<>();
        _innerTombstones = new HashMap<>();
        _outerTombstones = new HashMap<>();
        _isModified = false;

    }

    @JsonCreator
    private JoinQueryIndex(@JsonProperty("outerDocs") Map<String, Document> outerDocs,
                           @JsonProperty("innerDocs") Map<String, Document> innerDocs,
                           @JsonProperty("innerTombstones") Map<String, Set<Integer>> innerTombstones,
                           @JsonProperty("outerTombstones") Map<String, Set<Integer>> outerTombstones) {
        _outerDocs = checkNotNull(outerDocs, "outerDocs");
        _innerDocs = checkNotNull(innerDocs, "innerDocs");
        _innerTombstones = checkNotNull(innerTombstones, "innerTombstones");
        _outerTombstones = checkNotNull(outerTombstones, "outerTombstones");
        _isModified = false;

    }

    public void putDocument(Document document, QueryLevel queryLevel) {

        checkArgument(!_isModified, "Query Index can only be modified once");

        _modifiedDocument = document;
        _modifiedLevel = queryLevel;

        Map<String, Document> mapForInsert = queryLevel == QueryLevel.OUTER ? _outerDocs : _innerDocs;

        Document indexedDocument = mapForInsert.get(document.getKey());

        if (indexedDocument == null || document.getVersion() > indexedDocument.getVersion()) {
            mapForInsert.put(document.getKey(), document);
            _isModified = true;
        }
    }

    public void deleteDocument(Document document, QueryLevel queryLevel, boolean shouldTombstone) {
        checkArgument(!_isModified, "Query Index can only be modified once");

        _modifiedDocument = document;
        _modifiedLevel = queryLevel;
        _isModified = true;

        if (shouldTombstone) {
            if (queryLevel == QueryLevel.OUTER) {
                Set<Integer> tombstones = MoreObjects.firstNonNull(_outerTombstones.get(document.getKey()), new HashSet<>());
                tombstones.add(document.getVersion());
                _outerTombstones.put(document.getKey(), tombstones);
            } else {
                Set<Integer> tombstones = MoreObjects.firstNonNull(_innerTombstones.get(document.getKey()), new HashSet<>());
                tombstones.add(document.getVersion());
                _innerTombstones.put(document.getKey(), tombstones);
            }
        }

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
