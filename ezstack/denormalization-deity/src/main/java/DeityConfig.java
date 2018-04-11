import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;

public class DeityConfig extends JobConfig {

    public static final String DATADOG_KEY = "deity.datadog.key";
    public static final String ADJUSTMENT_PERIOD_MS = "deity.update.interval.ms";
    public static final String URI_ADDRESS = "deity.clientfactory.uri.address";
    public static final String CACHE_PERIOD_SECS = "deity.cache.interval.secs";
    public static final String UPDATE_QUERY_THRESHOLD = "deity.update.query.threshold";

    private static final long DEFAULT_ADJUSTMENT_PERIOD_MS = 3600000;
    private static final long DEFAULT_UPDATE_QUERY_THRESHOLD = 2000;
    private static final long DEFAULT_CACHE_PERIOD_SECS = 10;

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

    public long getCachePeriod() {return getLong(CACHE_PERIOD_SECS, DEFAULT_CACHE_PERIOD_SECS); }

    public long getUpdateQueryThreshold() {return getLong(UPDATE_QUERY_THRESHOLD, DEFAULT_UPDATE_QUERY_THRESHOLD); }
}