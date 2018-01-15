package org.ezstack.ezapp.datastore.api;

import com.datastax.driver.core.utils.UUIDs;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;

import java.util.Map;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class Document {

    private final String _table;
    private final String _key;
    private UUID _timestamp;
    private Map<String, Object> _data;
    private int _version;

    @JsonCreator
    public Document(@JsonProperty("_table") String table,
                    @JsonProperty("_key") String key, @JsonProperty("_timestamp") UUID timestamp,
                    @JsonProperty("_data") Map<String, Object> data, @JsonProperty("_version") int version) {

        checkNotNull(table, "table");
        checkNotNull(key, "key");
        checkNotNull(data, "data");
        checkArgument(Names.isLegalTableName(table), "Invalid Table Name");
        checkArgument(Names.isLegalKey("Invalid key"));

        _table = table;
        _key = key;
        _timestamp = MoreObjects.firstNonNull(timestamp, UUIDs.timeBased());
        _data = data;
        _version = version;
    }

    // This constructor should only be used if there is only one version of this document to consider
    public Document(Update update) {
        _table = update.getTable();
        _key = update.getKey();
        _timestamp = update.getTimestamp();
        _data = update.getData();
        _version = 1;
    }

    public void addUpdate(Update update) {
        checkArgument(_table.equals(update.getTable()) && _key.equals(update.getKey()),
                "Update is not to same record as existing document");

        // TODO: figure out a better way to maintain timestamp consistency, until then, simple assignment
        _timestamp = update.getTimestamp();

        _version++;

        if (update.isUpdate()) {
            resolveData(update.getData(), _data);
        } else {
            _data = update.getData();
        }
    }

    private static void resolveData(Map<String, Object> updatedData, Map<String, Object> dataToUpdate) {
        for (Map.Entry<String, Object> attribute : updatedData.entrySet()) {

            // TODO: define a better write model to conditionally update parts of a nested document, until then merge them attribute by attribute
            if (attribute.getValue() instanceof Map<?, ?> && dataToUpdate.get(attribute.getKey()) instanceof Map<?,?>) {
                resolveData((Map<String, Object>) attribute.getValue(), (Map<String, Object>) dataToUpdate.get(attribute.getKey()));
            } else {
                dataToUpdate.put(attribute.getKey(), attribute.getValue());
            }
        }

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

    @JsonProperty("_version")
    public int getVersion() {
        return _version;
    }
}
