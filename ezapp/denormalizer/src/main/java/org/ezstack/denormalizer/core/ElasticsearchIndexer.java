package org.ezstack.denormalizer.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.samza.operators.functions.SinkFunction;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;
import org.ezstack.denormalizer.model.Document;

import java.util.Map;

public class ElasticsearchIndexer implements SinkFunction<Document> {

    private static final ObjectMapper _mapper = new ObjectMapper();

    @Override
    public void apply(Document document, MessageCollector messageCollector, TaskCoordinator taskCoordinator) {

        messageCollector.send(new OutgoingMessageEnvelope(new SystemStream("elasticsearch", document.getTable() + "/" + document.getTable()),
                document.getKey(), _mapper.convertValue(document, Map.class)));
    }
}
