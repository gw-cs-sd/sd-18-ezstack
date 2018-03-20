package org.ezstack.ezapp.datastore.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.NotEmpty;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import static com.google.common.base.MoreObjects.firstNonNull;

public class Rule {

    public enum RuleStatus {
        PENDING,
        ACCEPTED,
        ACTIVE,
        INACTIVE
    }

    private final Query _query;

    private final String _table;

    private RuleStatus _status;

    @JsonCreator
    private Rule(@NotEmpty @JsonProperty("query") Query query,
                 @NotEmpty @JsonProperty("table") String table,
                 @JsonProperty("status") RuleStatus status) {
        checkNotNull(query, "query");
        checkNotNull(table, "table");
        checkArgument(Names.isLegalTableName(table), "Invalid table name");
        checkArgument(isDenormalizableQuery(query), "Undenormalizable query");

        _query = query;
        _table = table;
        _status = firstNonNull(status, RuleStatus.PENDING);
    }

    public Rule(@NotEmpty Query query, @NotEmpty String table) {
        this(query, table, RuleStatus.PENDING);
    }

    public Rule(@NotEmpty Query query) {
        this(checkNotNull(query, "query"), query.getMurmur3HashAsString(), RuleStatus.PENDING);
    }

    private static boolean isDenormalizableQuery(Query query) {

        for (SearchType type : query.getSearchTypes()) {
            if (type.getType() != SearchType.Type.SEARCH) {
                return false;
            }
        }

        return query.getJoin() == null || query.getJoin().getJoin() == null;
    }

    public Query getQuery() {
        return _query;
    }

    public String getTable() {
        return _table;
    }

    public RuleStatus getStatus() {
        return _status;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Rule)) return false;
        Rule rule = (Rule) o;
        return Objects.equals(_query, rule._query) &&
                Objects.equals(_table, rule._table) &&
                _status == rule._status;
    }

    @Override
    public int hashCode() {
        return Objects.hash(_query, _table, _status);
    }

    @Override
    public String toString() {
        return "Rule{" +
                "_query=" + _query +
                ", _table='" + _table + '\'' +
                ", _status=" + _status +
                '}';
    }
}
