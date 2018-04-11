import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.AbstractScheduledService;
import org.ezstack.ezapp.datastore.api.Rule;
import org.ezstack.ezapp.datastore.api.RulesManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class RuleCreationService extends AbstractScheduledService {

    private DeityConfig _config;
    private final MetricRegistry _metrics;
    private final MetricRegistry.MetricSupplier<Histogram> _histogramSupplier;
    private Map<String, QueryObject> _priorityObjects;
    private RulesManager _rulesManager;
    private Supplier<Set<Rule>> _ruleSupplier;
    private final RuleDeterminationProcessor _ruleDeterminationProcessor;
    private final AtomicIntManager _intManager;
    private static final Logger LOG = LoggerFactory.getLogger(RuleCreationService.class);

    public RuleCreationService(DeityConfig config, MetricRegistry metrics, MetricRegistry.MetricSupplier<Histogram> histogramSupplier, Map<String, QueryObject> priorityObjects, RulesManager rulesManager, Supplier<Set<Rule>> ruleSupplier, AtomicIntManager intManager) {
        _config = config;
        _metrics = metrics;
        _histogramSupplier = histogramSupplier;
        _priorityObjects = priorityObjects;
        _rulesManager = rulesManager;
        _ruleSupplier = ruleSupplier;
        _intManager = intManager;
        _ruleDeterminationProcessor = new RuleDeterminationProcessor(_metrics, _histogramSupplier, _priorityObjects, _rulesManager, _ruleSupplier);
    }

    @Override
    protected void runOneIteration() throws Exception {
        if(_intManager.getAndZero() >= _config.getUpdateQueryThreshold()) {
            _ruleDeterminationProcessor.ruleCreationProcess();
        }
    }

    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedDelaySchedule(100, _config.getAdjustmentPeriod(), TimeUnit.MILLISECONDS);
    }
}
