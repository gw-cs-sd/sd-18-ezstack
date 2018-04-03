package org.ezstack.denormalizer.model;

import com.google.common.base.Objects;
import org.ezstack.ezapp.datastore.api.Rule;

public class RuleIndexPair {
    private final Rule _rule;
    private final QueryLevel _queryLevel;

    public RuleIndexPair(Rule rule, QueryLevel queryLevel) {
        _rule = rule;
        _queryLevel = queryLevel;
    }

    public Rule getRule() {
        return _rule;
    }

    public QueryLevel getLevel() {
        return _queryLevel;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RuleIndexPair)) return false;
        RuleIndexPair that = (RuleIndexPair) o;
        return Objects.equal(_rule, that.getRule()) &&
                _queryLevel == that.getLevel();
    }
}
