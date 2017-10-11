package org.ezstack.ezapp.web.resources;

import com.codahale.metrics.health.HealthCheck;

public class EZHealthCheck extends HealthCheck{

    public EZHealthCheck() {
    }

    @Override
    protected Result check() throws Exception {
        // TODO: implement checks to ensure connections to kafka and elasticsearch
        return Result.healthy();
    }
}
