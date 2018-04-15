package org.ezstack.deity;

import org.apache.samza.config.Config;
import org.apache.samza.operators.functions.SinkFunction;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.ezstack.ezapp.querybus.api.QueryMetadata;

import static com.google.common.base.Preconditions.checkNotNull;

public class RuleCreationServiceSamzaWrapper implements SinkFunction<QueryMetadata> {

    private final RuleCreationService _ruleCreationService;

    public RuleCreationServiceSamzaWrapper(RuleCreationService ruleCreationService) {
        _ruleCreationService = checkNotNull(ruleCreationService, "ruleCreationService");
    }

    // No-Op
    @Override
    public void apply(QueryMetadata q, MessageCollector m, TaskCoordinator t) { }

    @Override
    public void close() {
        _ruleCreationService.stopAsync().awaitTerminated();
    }

    @Override
    public void init(Config config, TaskContext context) {
        _ruleCreationService.startAsync().awaitRunning();
    }
}
