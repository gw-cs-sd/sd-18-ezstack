package org.ezstack.denormalizer.model;

import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.databind.util.ISO8601Utils;
import com.google.common.base.Preconditions;
import org.ezstack.ezapp.datastore.api.DataType;
import org.ezstack.ezapp.datastore.api.Names;
import org.ezstack.ezapp.datastore.api.Update;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class Document {

    private static final long NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L;

    private String _table;
    private final String _key;
    private final UUID _firstUpdateAt;
    private UUID _lastUpdateAt;
    private Map<String, Object> _data;
    private int _version;

    private boolean _hasMutated;

    @JsonCreator
    public Document(@JsonProperty("~table") String table, @JsonProperty("~key") String key,
                    @JsonProperty("~firstUpdateAt") UUID firstUpdateAt, @JsonProperty("~lastUpdateAt") UUID lastUpdateAt,
                    @JsonProperty("~version") int version) {

        checkNotNull(table, "table");
        checkNotNull(key, "key");
        checkNotNull(firstUpdateAt, "firstUpdateAt");
        checkNotNull(lastUpdateAt, "lastUpdateAt");
        Preconditions.checkArgument(Names.isLegalTableName(table), "Invalid Table Name");
        checkArgument(Names.isLegalKey("Invalid key"));

        _table = table;
        _key = key;
        _firstUpdateAt = firstUpdateAt;
        _lastUpdateAt = lastUpdateAt;
        _data = new HashMap<>();
        _version = version;

        _hasMutated = false;
    }

    /** This constructor should only be used if there is only one version of this document to consider */
    public Document(Update update) {
        _table = update.getTable();
        _key = update.getKey();
        _firstUpdateAt = update.getTimestamp();
        _lastUpdateAt = update.getTimestamp();
        _data = update.getData();
        _version = 1;

        _hasMutated = false;
    }

    /** lastUpdateAt and version are only updated if the document data is actually modified by the update */
    public void addUpdate(Update update) {
        checkArgument(_table.equals(update.getTable()) && _key.equals(update.getKey()),
                "Update is not to same record as existing document");

        if (update.isUpdate()) {
            resolveData(update.getData(), _data, update.getTimestamp());
        } else {
            _data = update.getData();
            _hasMutated = true;
        }

        if(_hasMutated) {
            _version++;
            _lastUpdateAt = update.getTimestamp();
            _hasMutated = false;
        }
    }

    @JsonIgnore
    public void setTable(String table) {
        checkArgument(Names.isLegalTableName(table), "Invalid Table Name");
        _table = table;
    }

    private void resolveData(Map<String, Object> updatedData, Map<String, Object> dataToUpdate, UUID updatedDataTimestamp) {

        for (Map.Entry<String, Object> attribute : updatedData.entrySet()) {
            Object updatedVal = attribute.getValue();
            Object oldVal = dataToUpdate.get(attribute.getKey());

            if (DataType.getDataType(updatedVal) == DataType.JsonTypes.MAP && DataType.getDataType(oldVal) == DataType.JsonTypes.MAP) {
                resolveData((Map<String, Object>) updatedVal, (Map<String, Object>) oldVal, updatedDataTimestamp);
            } else {
                if (updatedVal != null ? !updatedVal.equals(oldVal) : updatedVal != oldVal) {
                    _hasMutated = true;
                    dataToUpdate.put(attribute.getKey(), updatedVal);
                }
            }
        }

    }


    private static String asISOTimestamp(UUID timeUuid) {
        return (timeUuid != null) ? ISO8601Utils.format(new Date(getTimeMillis(timeUuid)), true) : null;
    }

    private static long getTimeMillis(UUID uuid) {
        return (uuid.timestamp() - NUM_100NS_INTERVALS_SINCE_UUID_EPOCH) / 10000;
    }

    @JsonIgnore
    public String getFirstUpdateAt() {
        return asISOTimestamp(_firstUpdateAt);
    }

    @JsonIgnore
    public String getLastUpdateAt() {
        return asISOTimestamp(_lastUpdateAt);
    }

    @JsonProperty("~table")
    public String getTable() {
        return _table;
    }

    @JsonProperty("~key")
    public String getKey() {
        return _key;
    }

    @JsonProperty("~firstUpdateAt")
    public UUID getFirstUpdateAtUUID() {
        return _firstUpdateAt;
    }

    @JsonProperty("~lastUpdateAt")
    public UUID getLastUpdateAtUUID() {
        return _lastUpdateAt;
    }

    @JsonProperty("~version")
    public int getVersion() {
        return _version;
    }

    @JsonAnyGetter
    public Map<String, Object> getData() {
        return _data;
    }

    @JsonAnySetter
    public void setDataField(String key, Object value) {
        _data.put(key, value);
    }

    public Object getValue(String key) {
        return _data.get(key);
    }
}
