import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;

public class DeityConfig extends JobConfig {

    public static final String DATADOG_KEY = "deity.datadog.key";
    public static final String ADJUSTMENT_PERIOD_MS = "deity.update.interval.ms";
    public static final String URI_ADDRESS = "deity.clientfactory.uri.address";

    private static final long DEFAULT_ADJUSTMENT_PERIOD_MS = 86400000;

    public DeityConfig(Config config) {
        super(config);
    }

    public String getDatadogKey() {
        return get(DATADOG_KEY);
    }

    public long getAdjustmentPeriod() {
        return getLong(ADJUSTMENT_PERIOD_MS, DEFAULT_ADJUSTMENT_PERIOD_MS);
    }

    public String getUriAddress() {
        return get(URI_ADDRESS);
    }
}