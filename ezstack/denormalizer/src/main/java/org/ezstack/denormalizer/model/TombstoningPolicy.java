package org.ezstack.denormalizer.model;

import org.ezstack.ezapp.datastore.api.Rule;

import static com.google.common.base.Preconditions.checkNotNull;

public enum TombstoningPolicy {
    NO_TOMBSTONING,
    TOMBSTONE_ALL,
    BASED_ON_RULE;

    public static boolean shouldTombstone(TombstoningPolicy tombstoningPolicy, Rule rule) {
        checkNotNull(rule);
        switch (tombstoningPolicy) {
            case NO_TOMBSTONING:
                return false;
            case TOMBSTONE_ALL:
                return true;
            case BASED_ON_RULE:
                return rule.getStatus() != Rule.RuleStatus.ACTIVE && rule.getStatus() != Rule.RuleStatus.INACTIVE;
            default:
                throw new NullPointerException("Tombstoning policy cannot be null");
        }
    }
}
