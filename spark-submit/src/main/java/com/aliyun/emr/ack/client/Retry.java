package com.aliyun.emr.ack.client;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import org.apache.http.NoHttpResponseException;
import org.apache.http.conn.ConnectTimeoutException;

/**
 * Minimal retry engine with exponential backoff + full jitter, plus the failure-classification
 * policies used by the submission chain.
 *
 * <p>Two policies are provided:
 *
 * <ul>
 *   <li>{@link #isTransientNetwork} — for idempotent operations (OSS PUT, Kyuubi file upload):
 *       retries any transient network error plus 5xx/429.
 *   <li>{@link #isConnectPhaseOnly} — for the non-idempotent batch submit: retries ONLY
 *       connection-establishment failures (request provably not sent), so a lost response can never
 *       cause a duplicate Spark job.
 * </ul>
 *
 * <p>Interruption during backoff is signalled with the dedicated {@link RetryInterruptedException}
 * (NOT {@link java.io.InterruptedIOException}, which is the parent of {@link
 * SocketTimeoutException}/{@link ConnectTimeoutException} and would make read/connect timeouts
 * indistinguishable from Ctrl-C).
 */
public final class Retry {

    private Retry() {}

    private static final Random RANDOM = new Random();

    /** A unit of work that may fail with an IOException. */
    @FunctionalInterface
    public interface RetryableOp<T> {
        T call() throws IOException;
    }

    /** Decides whether a given failure is worth retrying. */
    @FunctionalInterface
    public interface RetryPredicate {
        boolean isRetryable(IOException e);
    }

    /** Thrown when the backoff sleep is interrupted (maps to exit code 130 at the top level). */
    public static final class RetryInterruptedException extends IOException {
        public RetryInterruptedException(String message) {
            super(message);
        }
    }

    /** Immutable retry parameters. Values are clamped to safe ranges on construction. */
    public static final class RetryConfig {
        final int maxAttempts;
        final long initialBackoffMs;
        final long maxBackoffMs;
        final double multiplier;
        final RetryPredicate predicate;

        public RetryConfig(
                int maxAttempts,
                long initialBackoffMs,
                long maxBackoffMs,
                double multiplier,
                RetryPredicate predicate) {
            this.maxAttempts = Math.max(1, maxAttempts);
            this.initialBackoffMs = Math.max(0L, initialBackoffMs);
            this.maxBackoffMs = Math.max(this.initialBackoffMs, maxBackoffMs);
            this.multiplier = multiplier >= 1.0 ? multiplier : 1.0;
            this.predicate = predicate;
        }
    }

    /**
     * Run {@code op}, retrying transient failures per {@code cfg}. On a retryable failure that has
     * attempts remaining, logs and sleeps (with jitter) before retrying; otherwise rethrows the
     * original exception unchanged (preserving its message and any {@link HttpStatusException}
     * status code).
     */
    public static <T> T execute(String opName, RetryConfig cfg, RetryableOp<T> op)
            throws IOException {
        int attempt = 0;
        while (true) {
            attempt++;
            try {
                return op.call();
            } catch (IOException e) {
                if (!cfg.predicate.isRetryable(e) || attempt >= cfg.maxAttempts) {
                    throw e;
                }
                long sleepMs = computeBackoffWithJitter(cfg, attempt);
                System.err.println(
                        "["
                                + ts()
                                + "] "
                                + opName
                                + " failed (attempt "
                                + attempt
                                + "/"
                                + cfg.maxAttempts
                                + "): "
                                + e.getMessage()
                                + ", retrying in "
                                + sleepMs
                                + "ms");
                sleep(sleepMs, opName);
            }
        }
    }

    /** Full jitter: random value in [0, min(maxBackoff, initial * multiplier^(attempt-1))]. */
    static long computeBackoffWithJitter(RetryConfig cfg, int attempt) {
        double exp = (double) cfg.initialBackoffMs * Math.pow(cfg.multiplier, attempt - 1);
        double cap = Math.min((double) cfg.maxBackoffMs, exp);
        if (cap <= 0) {
            return 0L;
        }
        return (long) (RANDOM.nextDouble() * cap);
    }

    private static void sleep(long ms, String opName) throws IOException {
        if (ms <= 0) {
            return;
        }
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RetryInterruptedException("Interrupted during retry backoff of " + opName);
        }
    }

    /**
     * Retryable for idempotent operations: transient network errors and 5xx/429. {@link
     * SSLHandshakeException} is excluded (invalid cert / protocol mismatch are stable failures;
     * retrying only delays the inevitable).
     */
    public static boolean isTransientNetwork(IOException e) {
        if (e instanceof SSLHandshakeException) {
            return false;
        }
        if (e instanceof ConnectException
                || e instanceof ConnectTimeoutException
                || e instanceof UnknownHostException
                || e instanceof SocketTimeoutException
                || e instanceof NoHttpResponseException
                || e instanceof SSLException) {
            return true;
        }
        if (e instanceof HttpStatusException) {
            int sc = ((HttpStatusException) e).getStatusCode();
            return sc >= 500 || sc == 429;
        }
        return false;
    }

    /**
     * Retryable for the non-idempotent batch submit: ONLY connection-establishment failures where
     * the request body was provably never sent. Read timeouts, {@code NoHttpResponseException}, 5xx
     * and 429 are intentionally NOT retried — the batch may already have been created, and
     * resending would duplicate it.
     */
    public static boolean isConnectPhaseOnly(IOException e) {
        return e instanceof ConnectException
                || e instanceof ConnectTimeoutException
                || e instanceof UnknownHostException;
    }

    private static String ts() {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
    }
}
