package org.ezstack.denormalizer.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;

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
        _outerDocs = new HashMap<>();
        _innerDocs = new HashMap<>();

    }

    @JsonCreator
    private JoinQueryIndex(@JsonProperty("outerDocs") Map<String, Document> outerDocs,
                           @JsonProperty("innerDocs") Map<String, Document> innerDocs) {
        _outerDocs = outerDocs;
        _innerDocs = innerDocs;

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

        if (queryLevel == QueryLevel.OUTER) {
            _outerDocs.remove(document.getKey());
        }
    }

    // TODO: just discovered a possible race condition. If an outer document changes partitions, there is a chance that
    // its deletion record end up in elasticsearch after its update from a different partition. This could be distastrous,
    // as it would lead to missing data. This can occur because both the deletion and the insertion affect the same
    // key in elasticsearch.

    // Maybe the solution is some kind of delete only if the version matches? I believe elasticsearch has this capability
}
