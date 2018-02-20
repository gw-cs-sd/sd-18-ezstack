package org.ezstack.denormalizer.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.samza.operators.functions.SinkFunction;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;
import org.ezstack.denormalizer.model.WritableResult;

import java.util.Map;

public class ElasticsearchIndexer implements SinkFunction<WritableResult> {

    private static final ObjectMapper _mapper = new ObjectMapper();

    @Override
    public void apply(WritableResult resultMsg, MessageCollector messageCollector, TaskCoordinator taskCoordinator) {

        messageCollector.send(new OutgoingMessageEnvelope(
                new SystemStream("elasticsearch", resultMsg.getTable() + "/" + resultMsg.getTable()),
                resultMsg.getDocument().getKey(), _mapper.convertValue(resultMsg.getDocument(), Map.class)));
    }
}
