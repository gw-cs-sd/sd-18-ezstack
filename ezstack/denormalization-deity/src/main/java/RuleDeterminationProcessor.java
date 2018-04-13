import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import org.ezstack.ezapp.datastore.api.Rule;
import org.ezstack.ezapp.datastore.api.RulesManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

public class RuleDeterminationProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(RuleDeterminationProcessor.class);

    private final DeityMetricRegistry _metrics;
    private final DeityMetricRegistry.MetricSupplier<Histogram> _histogramSupplier;
    private RulesManager _rulesManager;
    private Supplier<Set<Rule>> _ruleSupplier;

    public RuleDeterminationProcessor(DeityMetricRegistry metrics, DeityMetricRegistry.MetricSupplier<Histogram> histogramSupplier, RulesManager rulesManager, Supplier<Set<Rule>> ruleSupplier) {
        _metrics = metrics;
        _histogramSupplier = histogramSupplier;
        _rulesManager = rulesManager;
        _ruleSupplier = ruleSupplier;
    }

    public void ruleCreationProcess() {
        long threshold = startRuleCreation();
        addRules(threshold);
    }

    /**
     * This function is for determining the score threshold that the entire querybase is judged on. It makes and updates
     * a histogram of all scores, so that the threshold will always be up-to-date with the current state of the system.
     * @return
     */
    private long startRuleCreation() {
        Histogram histogram = _metrics.histogram("baseline", _histogramSupplier);
        Snapshot snap = histogram.getSnapshot();

        for (Map.Entry<String, QueryObject> entry : _metrics.getQueryObjects().entrySet()) {
            QueryObject value = entry.getValue();
            histogram.update(value.getPriority());
        }

        return (long)snap.get75thPercentile();
    }

    /**
     * This function runs through the maintained list of unique queries and uses their calculated scores to determine
     * which rules are to be implemented by the denormalizer.
     * @param threshold
     */
    private void addRules(long threshold) {
        for (Map.Entry<String, QueryObject> entry : _metrics.getQueryObjects().entrySet()) {
            QueryObject value = entry.getValue();
            QueryToRule ruleConverter = new QueryToRule(_rulesManager, _ruleSupplier);

            //we check to see which rules are necessary to be implemented
            if (value.getPriority() >= threshold) {
                Rule rule = ruleConverter.convertToRule(value.getQuery());
                if (rule != null) {
                    LOG.info("The rule for Query \"" + value.getQuery().toString() +  "\" is being added.");
                    ruleConverter.addRule(rule);
                }
            }
            else {
                //TODO: if the rule exists and is no longer necessary, remove the rule --- THIS IS NOT IMPLEMENTED YET
            }
        }
    }

}
