package org.ezstack.ezapp.writer.api;

import com.datastax.driver.core.utils.UUIDs;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;

import java.util.Map;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class Update {
    private final String _table;
    private final String _key;
    private final UUID _timestamp;
    private final Map<String, Object> _data;

    @JsonCreator
    public Update(@JsonProperty("_table") String table, @JsonProperty("_key") String key,
                  @JsonProperty("_timestamp") UUID timestamp, @JsonProperty("_data") Map<String, Object> data) {

        checkNotNull(table, "table");
        checkNotNull(key, "key");
        checkNotNull(data, "data");
        checkArgument(Names.isLegalTableName(table), "Invalid Table Name");
        checkArgument(Names.isLegalKey("Invalid key"));

        _table = table;
        _key = key;
        _timestamp = MoreObjects.firstNonNull(timestamp, UUIDs.timeBased());
        _data = data;
    }

    @JsonProperty("_table")
    public String getTable() {
        return _table;
    }

    @JsonProperty("_key")
    public String getKey() {
        return _key;
    }

    @JsonProperty("_timestamp")
    public UUID getTimestamp() {
        return _timestamp;
    }

    @JsonProperty("_data")
    public Map<String, Object> getData() {
        return _data;
    }
}
