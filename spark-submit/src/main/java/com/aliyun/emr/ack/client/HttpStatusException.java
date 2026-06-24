package com.aliyun.emr.ack.client;

import java.io.IOException;

/**
 * IOException that carries the HTTP status code of a failed response, so retry logic can classify
 * failures (e.g. 5xx/429 transient vs 4xx permanent) without parsing the exception message.
 */
public class HttpStatusException extends IOException {
    private final int statusCode;

    public HttpStatusException(int statusCode, String message) {
        super(message);
        this.statusCode = statusCode;
    }

    public int getStatusCode() {
        return statusCode;
    }
}
