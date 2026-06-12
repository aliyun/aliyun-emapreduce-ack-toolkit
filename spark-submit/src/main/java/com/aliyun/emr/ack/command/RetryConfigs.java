package com.aliyun.emr.ack.command;

import com.aliyun.emr.ack.cli.*;
import com.aliyun.emr.ack.client.*;
import com.aliyun.emr.ack.util.*;

import java.io.IOException;
import java.util.Map;

/**
 * Builds the client-only retry policies (namespace {@code spark.submit.retry.*}) from the submit conf
 * and config file, and runs the batch submission under the submit policy.
 */
public final class RetryConfigs {
    private RetryConfigs() {
    }

    public static Retry.RetryConfig forSubmit(Map<String, String> conf, Config config) {
        boolean enabled = getBool("spark.submit.retry.enabled", true, conf, config);
        int maxAttempts = enabled ? getInt("spark.submit.retry.maxAttempts", 3, conf, config) : 1;
        long initial = getLong("spark.submit.retry.initialBackoffMs", 1000L, conf, config);
        long max = getLong("spark.submit.retry.maxBackoffMs", 8000L, conf, config);
        double multiplier = getDouble("spark.submit.retry.multiplier", 2.0, conf, config);
        // Submit is non-idempotent: retry ONLY connection-phase failures (never duplicates a job).
        return new Retry.RetryConfig(maxAttempts, initial, max, multiplier, Retry::isConnectPhaseOnly);
    }

    public static Retry.RetryConfig forUpload(Map<String, String> conf, Config config) {
        boolean enabled = getBool("spark.submit.retry.enabled", true, conf, config);
        int maxAttempts = enabled ? getInt("spark.submit.retry.upload.maxAttempts", 4, conf, config) : 1;
        long initial = getLong("spark.submit.retry.initialBackoffMs", 1000L, conf, config);
        long max = getLong("spark.submit.retry.maxBackoffMs", 8000L, conf, config);
        double multiplier = getDouble("spark.submit.retry.multiplier", 2.0, conf, config);
        // Uploads are idempotent (OSS PUT) or orphan-tolerant (Kyuubi upload): retry all transient errors.
        return new Retry.RetryConfig(maxAttempts, initial, max, multiplier, Retry::isTransientNetwork);
    }

    public static KyuubiClient.BatchResponse submit(
            KyuubiClient client, SparkSubmitArgs submitArgs, Retry.RetryConfig cfg) throws IOException {
        return Retry.execute("submitBatch", cfg, () -> client.submitBatch(submitArgs));
    }

    private static int getInt(String key, int def, Map<String, String> conf, Config config) {
        String v = Confs.value(key, conf, config);
        if (v == null) return def;
        try {
            return Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            System.err.println("[" + Console.timestamp() + "] Invalid " + key + "=" + v + ", using default " + def);
            return def;
        }
    }

    private static long getLong(String key, long def, Map<String, String> conf, Config config) {
        String v = Confs.value(key, conf, config);
        if (v == null) return def;
        try {
            return Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            System.err.println("[" + Console.timestamp() + "] Invalid " + key + "=" + v + ", using default " + def);
            return def;
        }
    }

    private static double getDouble(String key, double def, Map<String, String> conf, Config config) {
        String v = Confs.value(key, conf, config);
        if (v == null) return def;
        try {
            return Double.parseDouble(v.trim());
        } catch (NumberFormatException e) {
            System.err.println("[" + Console.timestamp() + "] Invalid " + key + "=" + v + ", using default " + def);
            return def;
        }
    }

    private static boolean getBool(String key, boolean def, Map<String, String> conf, Config config) {
        String v = Confs.value(key, conf, config);
        if (v == null) return def;
        return Boolean.parseBoolean(v.trim());
    }
}
