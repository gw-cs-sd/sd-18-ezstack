import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

public class Query {

    private final long _responseTime;
    private final String _strippedQuery;

    @JsonCreator
    public Query(@JsonProperty("responseTime") long responseTime, @JsonProperty("strippedQuery") String strippedQuery) {
        _responseTime = responseTime;
        _strippedQuery = strippedQuery;
    }

    @JsonProperty("strippedQuery")
    public String getStrippedQuery() {
        return _strippedQuery;
    }

    @JsonProperty("responseTime")
    public long getResponseTime() {
        return _responseTime;
    }
}
