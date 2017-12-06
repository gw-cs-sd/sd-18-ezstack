package org.ezstack.ezapp.denormalizer.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;

import java.util.List;

public class DenormalizedTable {

    private final String _denormalizedTableName;
    private final String _table;
    private final List<Denormalization> _denormalizations;

    @JsonCreator
    public DenormalizedTable(@JsonProperty("denormalizedTableName") String denormalizedTableName,
                             @JsonProperty("startTable") String startTable) {
        _table = startTable;
        _denormalizedTableName = denormalizedTableName;
        _denormalizations = Lists.newArrayList();
    }

    @JsonProperty("table")
    public String getTable() {
        return _table;
    }

    @JsonProperty("denormalizedTableName")
    public String getDenormalizedTableName() {
        return _denormalizedTableName;
    }

    public DenormalizedTable addDenormalization(Denormalization denormalization, String attribute) {
        _denormalizations.add(denormalization);
        return this;
    }

    @JsonProperty("denormalizations")
    public List<Denormalization> getDenormalizations() {
        return _denormalizations;
    }
}
