package com.aliyun.emr.ack.util;

/** Shared polling/heartbeat tuning for the batch and SQL-session monitor loops. */
public final class Polling {
    private Polling() {}

    /** Delay between status polls. */
    public static final long INTERVAL_MS = 2000L;

    /** Page size for log fetches. */
    public static final int LOG_FETCH_SIZE = 100;

    /** Minimum gap between idle "still running" heartbeat lines. */
    public static final long HEARTBEAT_LOG_INTERVAL_MS = 60 * 1000L;

    /** No-activity window after which a SQL operation is canceled. */
    public static final long HEARTBEAT_TIMEOUT_MS = 30 * 60 * 1000L;
}
