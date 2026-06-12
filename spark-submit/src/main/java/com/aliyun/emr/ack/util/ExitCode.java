package com.aliyun.emr.ack.util;

/**
 * Process exit codes used across the CLI. 124/130 follow the shell conventions for timeout/SIGINT.
 */
public final class ExitCode {
    private ExitCode() {}

    public static final int SUCCESS = 0;
    public static final int ERROR = 1;
    public static final int TIMEOUT = 124;
    public static final int INTERRUPTED = 130;
}
