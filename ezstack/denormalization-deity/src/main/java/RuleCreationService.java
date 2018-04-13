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
    private final DeityMetricRegistry _queryMetricRegistry;
    private final DeityMetricRegistry.MetricSupplier<Histogram> _histogramSupplier;
    private RulesManager _rulesManager;
    private Supplier<Set<Rule>> _ruleSupplier;
    private final RuleDeterminationProcessor _ruleDeterminationProcessor;
    private final QueryCounter _intManager;
    private static final Logger LOG = LoggerFactory.getLogger(RuleCreationService.class);

    public RuleCreationService(DeityConfig config, DeityMetricRegistry metrics, DeityMetricRegistry.MetricSupplier<Histogram> histogramSupplier, RulesManager rulesManager, Supplier<Set<Rule>> ruleSupplier, QueryCounter intManager) {
        _config = config;
        _queryMetricRegistry = metrics;
        _histogramSupplier = histogramSupplier;
        _rulesManager = rulesManager;
        _ruleSupplier = ruleSupplier;
        _intManager = intManager;
        _ruleDeterminationProcessor = new RuleDeterminationProcessor(_queryMetricRegistry, _histogramSupplier, _rulesManager, _ruleSupplier);
    }

    /**
     * This service calls on the ruleDeterminationProcessor once every adjustment period (configurable by the user) to
     * update the rules stored by the system, however it will only trigger if a sufficient amount of new queries have
     * been made since the last execution.
     * @throws Exception
     */
    @Override
    protected void runOneIteration() throws Exception {
        if (_intManager.getAndReset() >= _config.getUpdateQueryThreshold()) {
            _ruleDeterminationProcessor.ruleCreationProcess();
        }
    }

    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedDelaySchedule(_config.getAdjustmentPeriod(), _config.getAdjustmentPeriod(), TimeUnit.SECONDS);
    }
}
