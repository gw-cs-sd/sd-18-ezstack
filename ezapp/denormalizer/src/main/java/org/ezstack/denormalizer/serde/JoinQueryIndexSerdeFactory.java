package org.ezstack.denormalizer.serde;

import org.apache.samza.config.Config;
import org.apache.samza.serializers.SerdeFactory;
import org.ezstack.denormalizer.model.JoinQueryIndex;

public class JoinQueryIndexSerdeFactory implements SerdeFactory<JoinQueryIndex> {
    
    @Override
    public JsonSerdeV3<JoinQueryIndex> getSerde(String name, Config config) {
        return new JsonSerdeV3<>(JoinQueryIndex.class);
    }
}
