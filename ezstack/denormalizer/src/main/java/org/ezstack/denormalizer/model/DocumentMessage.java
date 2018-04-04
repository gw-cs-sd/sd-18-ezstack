package org.ezstack.denormalizer.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.ezstack.ezapp.datastore.api.Document;
import org.ezstack.ezapp.datastore.api.Query;

import static org.ezstack.ezapp.datastore.api.Rule.RuleStatus;
import static com.google.common.base.Preconditions.checkNotNull;

public class DocumentMessage {

    private final Document _document;
    private final String _partitionKey;
    private final QueryLevel _level;
    private final OpCode _opCode;
    private final Query _query;
    private final RuleStatus _ruleStatus;


    @JsonCreator
    public DocumentMessage(@JsonProperty("document") Document document,
                           @JsonProperty("partitionKey") String partitionKey,
                           @JsonProperty("documentLevel") QueryLevel level,
                           @JsonProperty("opCode") OpCode opCode,
                           @JsonProperty("query") Query query,
                           @JsonProperty("ruleStatus") RuleStatus ruleStatus) {
        _document = checkNotNull(document);
        _partitionKey = checkNotNull(partitionKey);
        _level = checkNotNull(level);
        _opCode = checkNotNull(opCode);
        _query = checkNotNull(query);
        _ruleStatus = checkNotNull(ruleStatus);

    }

    public Document getDocument() {
        return _document;
    }

    public String getPartitionKey() {
        return _partitionKey;
    }

    public QueryLevel getDocumentLevel() {
        return _level;
    }

    public OpCode getOpCode() {
        return _opCode;
    }

    public Query getQuery() {
        return _query;
    }

    public RuleStatus getRuleStatus() {
        return _ruleStatus;
    }
}
