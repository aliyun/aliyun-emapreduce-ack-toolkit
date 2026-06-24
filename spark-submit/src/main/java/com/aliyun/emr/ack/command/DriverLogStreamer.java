package com.aliyun.emr.ack.command;

import com.aliyun.emr.ack.cli.*;
import com.aliyun.emr.ack.client.*;
import com.aliyun.emr.ack.util.*;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Streams a batch's Spark driver pod log to stdout in the background. It opens the Kyuubi SSE
 * endpoint, prints each {@code log} event, and reconnects across transient drops or stream-side
 * timeouts (idle / max-duration) for as long as the batch is still running, so a long-lived job
 * keeps tailing. It self-terminates once the batch reaches a terminal state, and the monitoring
 * loop also calls {@link #stop()} to guarantee a prompt, ordered shutdown before the final summary.
 */
public final class DriverLogStreamer {

    private static final int TAIL_LINES = 200; // initial backfill on first connect
    private static final int RECONNECT_LOOKBACK_SECONDS =
            2; // small overlap to bridge a reconnect gap
    private static final long RECONNECT_BACKOFF_MS = 2000L;
    private static final int MAX_CONSECUTIVE_ERRORS = 5; // give up + fall back to submission log
    private static final long STOP_JOIN_MS = 3000L;
    private static final int FINAL_BACKFILL_TAIL = 100_000; // effectively the whole driver log

    /**
     * Start streaming on a background daemon thread, or return null when disabled via {@code
     * --no-driver-log}. The returned streamer keeps the stream open across reconnects until the
     * batch finishes or {@link #stop()} is called.
     */
    public static DriverLogStreamer start(
            KyuubiClient client, SparkSubmitArgs submitArgs, String batchId) {
        if (!submitArgs.isDriverLogStream()) {
            return null;
        }
        DriverLogFilter filter =
                DriverLogFilter.fromRegexes(
                        submitArgs.getDriverLogGrep(), submitArgs.getDriverLogGrepV());
        DriverLogStreamer streamer = new DriverLogStreamer(client, batchId, filter);
        streamer.startThread();
        return streamer;
    }

    /**
     * Fill driver-log settings from the config file when not given on the command line, so common
     * filters can live in ~/.spark-submit.conf. CLI flags always win. Config keys (client-only,
     * never forwarded to Spark): {@code spark.submit.driver.log.enabled} (true/false; default
     * true), {@code spark.submit.driver.log.grep} (whitelist regex), {@code
     * spark.submit.driver.log.grep-v} (blacklist regex).
     */
    public static void applyConfigDefaults(SparkSubmitArgs submitArgs, Config config) {
        if (submitArgs.getDriverLogStream() == null) {
            String enabled = config.getProperty("spark.submit.driver.log.enabled");
            if (enabled != null && !enabled.trim().isEmpty()) {
                submitArgs.setDriverLogStream(Boolean.parseBoolean(enabled.trim()));
            }
        }
        if (submitArgs.getDriverLogGrep() == null) {
            String grep = config.getProperty("spark.submit.driver.log.grep");
            if (grep != null && !grep.trim().isEmpty()) {
                submitArgs.setDriverLogGrep(grep.trim());
            }
        }
        if (submitArgs.getDriverLogGrepV() == null) {
            String grepV = config.getProperty("spark.submit.driver.log.grep-v");
            if (grepV != null && !grepV.trim().isEmpty()) {
                submitArgs.setDriverLogGrepV(grepV.trim());
            }
        }
    }

    /**
     * Validate the {@code --driver-log-grep[-v]} regexes, throwing {@link IllegalArgumentException}
     * (naming the offending flag) so the caller can fail fast before submitting a job.
     */
    public static void validateFilters(SparkSubmitArgs submitArgs) {
        DriverLogFilter.fromRegexes(submitArgs.getDriverLogGrep(), submitArgs.getDriverLogGrepV());
    }

    private final KyuubiClient client;
    private final String batchId;
    private final DriverLogFilter filter;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicLong lastActivityMillis = new AtomicLong(System.currentTimeMillis());
    // received any log line at all — drives the backfill/reconnect decision in run()
    private final AtomicBoolean seenAnyLine = new AtomicBoolean(false);
    // printed the streaming header — deferred until the first line that passes the filter
    private final AtomicBoolean headerPrinted = new AtomicBoolean(false);
    private final AtomicInteger consecutiveErrors = new AtomicInteger(0);
    private volatile boolean fallenBack = false;
    private volatile Runnable aborter;
    private Thread thread;

    // Package-private (not private) so the backfill path can be unit-tested without a live stream.
    DriverLogStreamer(KyuubiClient client, String batchId, DriverLogFilter filter) {
        this.client = client;
        this.batchId = batchId;
        this.filter = filter;
    }

    private void startThread() {
        thread = new Thread(this::run, "driver-log-stream");
        thread.setDaemon(true);
        thread.start();
    }

    /** Epoch millis of the most recent driver log line, used to suppress redundant heartbeats. */
    long lastActivityMillis() {
        return lastActivityMillis.get();
    }

    /** True once streaming gave up (feature disabled or repeated errors) and fell back. */
    boolean hasFallenBack() {
        return fallenBack;
    }

    void stop() {
        running.set(false);
        Runnable currentAborter = aborter;
        if (currentAborter != null) {
            try {
                currentAborter.run(); // unblock a reader parked on the socket
            } catch (RuntimeException ignore) {
                // best effort
            }
        }
        Thread current = thread;
        if (current != null) {
            try {
                current.join(STOP_JOIN_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void run() {
        KyuubiClient.DriverLogHandler handler = makeHandler("\n=== Driver Log (streaming) ===");

        while (running.get()) {
            KyuubiClient.DriverLogStreamResult result;
            try {
                // Backfill the tail until the first real log line arrives (an early connection can
                // end before the pod is ready); afterwards reconnect with only a small lookback so
                // we neither re-dump the whole log nor double the recent tail.
                boolean haveLogs = seenAnyLine.get();
                int tail = haveLogs ? 0 : TAIL_LINES;
                int since = haveLogs ? RECONNECT_LOOKBACK_SECONDS : 0;
                // timestamps=false: Spark log lines already carry their own timestamps, so the raw
                // line reads most like native driver output on the console.
                result =
                        client.streamDriverLog(
                                batchId, tail, since, false, a -> aborter = a, handler);
            } catch (IOException e) {
                result = KyuubiClient.DriverLogStreamResult.DISCONNECTED;
            }

            if (!running.get()) {
                return;
            }
            if (result == KyuubiClient.DriverLogStreamResult.DISABLED) {
                System.err.println(
                        "["
                                + Console.timestamp()
                                + "] [driver-log] server has driver log "
                                + "streaming disabled; falling back to the Kyuubi submission log.");
                fallenBack = true;
                return;
            }
            if (consecutiveErrors.get() >= MAX_CONSECUTIVE_ERRORS) {
                System.err.println(
                        "["
                                + Console.timestamp()
                                + "] [driver-log] giving up after repeated "
                                + "errors; falling back to the Kyuubi submission log.");
                fallenBack = true;
                return;
            }
            // ENDED or DISCONNECTED: the job may still be running, so reconnect — but stop once the
            // batch is terminal (the driver log is then complete and the pod is gone).
            try {
                if (client.getBatch(batchId).isFinished()) {
                    return;
                }
            } catch (IOException ignore) {
                // status check failed; reconnect anyway
            }
            if (!sleepQuietly(RECONNECT_BACKOFF_MS)) {
                return; // interrupted
            }
        }
    }

    /**
     * Build the SSE handler that prints driver-log lines through the filter, emitting {@code
     * header} the first time a line passes. Shared by the live stream and the final backfill so
     * both honour the same filter and print-the-header-once semantics.
     */
    private KyuubiClient.DriverLogHandler makeHandler(String header) {
        return new KyuubiClient.DriverLogHandler() {
            @Override
            public void onLog(String line, long timestampMillis) {
                // Mark activity on every received line regardless of whether the filter prints it.
                lastActivityMillis.set(System.currentTimeMillis());
                seenAnyLine.set(true);
                consecutiveErrors.set(0);
                if (!filter.shouldPrint(line)) {
                    return;
                }
                if (headerPrinted.compareAndSet(false, true)) {
                    System.out.println(header);
                }
                System.out.println(line);
            }

            @Override
            public void onEnd(String reason) {
                // The connection finished; run() decides whether to reconnect from batch status.
            }

            @Override
            public void onError(String message) {
                consecutiveErrors.incrementAndGet();
                System.err.println("[" + Console.timestamp() + "] [driver-log] " + message);
            }
        };
    }

    /** True once the live stream has received at least one driver log line. */
    boolean receivedAnyLine() {
        return seenAnyLine.get();
    }

    /**
     * After the batch is terminal, if the live stream never received a line — a short-lived pod can
     * finish before the background stream attaches — fetch the now-complete driver log once and
     * print it, so even a quick job shows its driver output. No-op once any line has streamed or
     * the stream already fell back to the submission log.
     */
    void drainFinalBackfill() {
        if (seenAnyLine.get() || fallenBack) {
            return;
        }
        try {
            client.streamDriverLog(
                    batchId,
                    FINAL_BACKFILL_TAIL,
                    0,
                    false,
                    a -> {},
                    makeHandler("\n=== Driver Log ==="));
        } catch (IOException e) {
            // Best effort: the final summary still prints, and a hard streaming failure would
            // already have fallen back to the submission log.
        }
    }

    private static boolean sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
