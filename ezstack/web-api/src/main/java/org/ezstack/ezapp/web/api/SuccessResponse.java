package org.ezstack.ezapp.web.api;

public class SuccessResponse {

    private static final SuccessResponse INSTANCE = new SuccessResponse();

    public static SuccessResponse instance() {
        return INSTANCE;
    }

    public boolean isSuccess() {
        return true;
    }
}
