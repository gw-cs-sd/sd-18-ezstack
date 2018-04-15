package org.ezstack.bootstrap.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.Map;

public class BootstrapClientResponse {
    private final String _jobId;
    private final boolean _success;
    private final Map<String, String> _properties;
    private final String _stdout;
    private final String _stderr;

    @JsonCreator
    public BootstrapClientResponse(@Valid @NotNull @JsonProperty("job.id") String jobId,
                                   @Valid @NotNull @JsonProperty("success") boolean success,
                                   @Valid @NotNull @JsonProperty("properties") Map<String, String> properties,
                                   @Valid @NotNull @JsonProperty("stdout") String stdout,
                                   @Valid @NotNull @JsonProperty("stderr") String stderr) {
        _jobId = jobId;
        _success = success;
        _properties = properties;
        _stdout = stdout;
        _stderr = stderr;
    }

    @JsonProperty("job.id")
    public String getJobId() {
        return _jobId;
    }

    public boolean isSuccess() {
        return _success;
    }

    public Map<String, String> getProperties() {
        return _properties;
    }

    public String getStdout() {
        return _stdout;
    }

    public String getStderr() {
        return _stderr;
    }

    @Override
    public String toString() {
        return "BootstrapClientResponse{" +
                "job.id='" + _jobId + '\'' +
                ", success=" + _success +
                ", properties=" + _properties +
                ", stdout='" + _stdout + '\'' +
                ", stderr='" + _stderr + '\'' +
                '}';
    }
}
