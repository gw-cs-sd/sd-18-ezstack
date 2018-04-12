import com.codahale.metrics.Histogram;
import com.google.common.util.concurrent.AbstractScheduledService;
import org.ezstack.ezapp.datastore.api.Rule;
import org.ezstack.ezapp.datastore.api.RulesManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class RuleCreationService extends AbstractScheduledService {

    private DeityConfig _config;
    private final DeityMetricRegistry _metrics;
    private final DeityMetricRegistry.MetricSupplier<Histogram> _histogramSupplier;
    private RulesManager _rulesManager;
    private Supplier<Set<Rule>> _ruleSupplier;
    private final RuleDeterminationProcessor _ruleDeterminationProcessor;
    private final AtomicIntManager _intManager;
    private static final Logger LOG = LoggerFactory.getLogger(RuleCreationService.class);

    public RuleCreationService(DeityConfig config, DeityMetricRegistry metrics, DeityMetricRegistry.MetricSupplier<Histogram> histogramSupplier, RulesManager rulesManager, Supplier<Set<Rule>> ruleSupplier, AtomicIntManager intManager) {
        _config = config;
        _metrics = metrics;
        _histogramSupplier = histogramSupplier;
        _rulesManager = rulesManager;
        _ruleSupplier = ruleSupplier;
        _intManager = intManager;
        _ruleDeterminationProcessor = new RuleDeterminationProcessor(_metrics, _histogramSupplier, _rulesManager, _ruleSupplier);
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
