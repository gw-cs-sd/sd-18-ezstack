import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;

public class DeityConfig extends JobConfig{

    public static final String DATADOG_KEY = "datadog.key";

    public DeityConfig(Config config) {
        super(config);
    }

    public String getDatadogKey() {
        return get(DATADOG_KEY);
    }
}