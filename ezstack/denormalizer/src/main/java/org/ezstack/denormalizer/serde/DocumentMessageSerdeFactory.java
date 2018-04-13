package org.ezstack.denormalizer.serde;

import org.apache.samza.config.Config;
import org.apache.samza.serializers.SerdeFactory;
import org.ezstack.denormalizer.model.DocumentMessage;

public class DocumentMessageSerdeFactory implements SerdeFactory<DocumentMessage> {
    @Override
    public JsonSerdeV3<DocumentMessage> getSerde(String name, Config config) {
        return new JsonSerdeV3<>(DocumentMessage.class);
    }
}
