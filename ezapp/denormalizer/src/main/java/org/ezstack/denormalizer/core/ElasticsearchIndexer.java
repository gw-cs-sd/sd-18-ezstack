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

    private static String getESDeletePath(String index, String type, int version) {
        return String.format("%s/%s/DELETE/%d", index, type, version);
    }

    @Override
    public void apply(WritableResult resultMsg, MessageCollector messageCollector, TaskCoordinator taskCoordinator) {

        switch (resultMsg.getOpCode()) {
            case UPDATE:
                messageCollector.send(new OutgoingMessageEnvelope(
                        new SystemStream("elasticsearch", resultMsg.getTable() + "/" + resultMsg.getTable()),
                        resultMsg.getDocument().getKey(), _mapper.convertValue(resultMsg.getDocument(), Map.class)));
                break;
            case DELETE:
                // Deletes must specify a version number to prevent the race condition where it accidentally deletes
                // an already updated document from a different container
                messageCollector.send(new OutgoingMessageEnvelope(
                        new SystemStream("elasticsearch",
                                getESDeletePath(resultMsg.getTable(), resultMsg.getTable(), resultMsg.getDocument().getVersion())),
                        resultMsg.getDocument().getKey(), _mapper.convertValue(resultMsg.getDocument(), Map.class)));
                break;
        }

    }
}
