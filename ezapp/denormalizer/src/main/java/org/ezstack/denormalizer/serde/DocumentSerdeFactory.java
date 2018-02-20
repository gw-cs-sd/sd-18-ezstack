package org.ezstack.denormalizer.serde;

import org.apache.samza.config.Config;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.SerdeFactory;
import org.ezstack.denormalizer.model.Document;

public class DocumentSerdeFactory implements SerdeFactory<Document> {
    @Override
    public JsonSerdeV3<Document> getSerde(String name, Config config) {
        return new JsonSerdeV3<>(Document.class);
    }
}
