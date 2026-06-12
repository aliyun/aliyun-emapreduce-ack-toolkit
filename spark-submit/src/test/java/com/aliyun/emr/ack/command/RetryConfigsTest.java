package com.aliyun.emr.ack.command;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.aliyun.emr.ack.client.Config;
import com.aliyun.emr.ack.client.HttpStatusException;
import com.aliyun.emr.ack.client.Retry;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class RetryConfigsTest {

    @Rule public TemporaryFolder tmp = new TemporaryFolder();

    @Test
    public void forSubmit_usesCliConfAndConnectPhaseOnlyPolicy() throws Exception {
        Map<String, String> conf = new HashMap<>();
        conf.put("spark.submit.retry.maxAttempts", "5");
        conf.put("spark.submit.retry.initialBackoffMs", "10");
        conf.put("spark.submit.retry.maxBackoffMs", "20");
        conf.put("spark.submit.retry.multiplier", "3.0");

        Retry.RetryConfig cfg = RetryConfigs.forSubmit(conf, new Config(missingConfig()));

        assertEquals(5, intField(cfg, "maxAttempts"));
        assertEquals(10L, longField(cfg, "initialBackoffMs"));
        assertEquals(20L, longField(cfg, "maxBackoffMs"));
        assertEquals(3.0, doubleField(cfg, "multiplier"), 1e-9);
        assertTrue(predicate(cfg).isRetryable(new ConnectException("refused")));
        assertFalse(predicate(cfg).isRetryable(new SocketTimeoutException("read timeout")));
        assertFalse(predicate(cfg).isRetryable(new HttpStatusException(503, "unavailable")));
    }

    @Test
    public void forUpload_usesUploadAttemptsAndTransientNetworkPolicyFromConfigFile()
            throws Exception {
        File f =
                writeConf(
                        "spark.submit.retry.upload.maxAttempts=7\n"
                                + "spark.submit.retry.initialBackoffMs=11\n"
                                + "spark.submit.retry.maxBackoffMs=44\n"
                                + "spark.submit.retry.multiplier=1.5\n");

        Retry.RetryConfig cfg =
                RetryConfigs.forUpload(
                        new HashMap<String, String>(), new Config(f.getAbsolutePath()));

        assertEquals(7, intField(cfg, "maxAttempts"));
        assertEquals(11L, longField(cfg, "initialBackoffMs"));
        assertEquals(44L, longField(cfg, "maxBackoffMs"));
        assertEquals(1.5, doubleField(cfg, "multiplier"), 1e-9);
        assertTrue(predicate(cfg).isRetryable(new SocketTimeoutException("read timeout")));
        assertTrue(predicate(cfg).isRetryable(new HttpStatusException(503, "unavailable")));
        assertFalse(predicate(cfg).isRetryable(new HttpStatusException(400, "bad request")));
    }

    @Test
    public void disabledRetryForcesSingleAttemptForBothPolicies() throws Exception {
        Map<String, String> conf = new HashMap<>();
        conf.put("spark.submit.retry.enabled", "false");
        conf.put("spark.submit.retry.maxAttempts", "9");
        conf.put("spark.submit.retry.upload.maxAttempts", "9");

        Config config = new Config(missingConfig());

        assertEquals(1, intField(RetryConfigs.forSubmit(conf, config), "maxAttempts"));
        assertEquals(1, intField(RetryConfigs.forUpload(conf, config), "maxAttempts"));
    }

    @Test
    public void invalidNumbersFallBackToDefaults() throws Exception {
        Map<String, String> conf = new HashMap<>();
        conf.put("spark.submit.retry.maxAttempts", "bad");
        conf.put("spark.submit.retry.initialBackoffMs", "bad");
        conf.put("spark.submit.retry.maxBackoffMs", "bad");
        conf.put("spark.submit.retry.multiplier", "bad");

        Retry.RetryConfig cfg = RetryConfigs.forSubmit(conf, new Config(missingConfig()));

        assertEquals(3, intField(cfg, "maxAttempts"));
        assertEquals(1000L, longField(cfg, "initialBackoffMs"));
        assertEquals(8000L, longField(cfg, "maxBackoffMs"));
        assertEquals(2.0, doubleField(cfg, "multiplier"), 1e-9);
    }

    private File writeConf(String content) throws IOException {
        File f = tmp.newFile("retry.conf");
        try (FileWriter w = new FileWriter(f)) {
            w.write(content);
        }
        return f;
    }

    private String missingConfig() {
        return tmp.getRoot().getAbsolutePath() + "/missing.conf";
    }

    private static int intField(Retry.RetryConfig cfg, String name) throws Exception {
        return ((Integer) field(cfg, name).get(cfg)).intValue();
    }

    private static long longField(Retry.RetryConfig cfg, String name) throws Exception {
        return ((Long) field(cfg, name).get(cfg)).longValue();
    }

    private static double doubleField(Retry.RetryConfig cfg, String name) throws Exception {
        return ((Double) field(cfg, name).get(cfg)).doubleValue();
    }

    private static Retry.RetryPredicate predicate(Retry.RetryConfig cfg) throws Exception {
        return (Retry.RetryPredicate) field(cfg, "predicate").get(cfg);
    }

    private static Field field(Retry.RetryConfig cfg, String name) throws Exception {
        Field f = cfg.getClass().getDeclaredField(name);
        f.setAccessible(true);
        return f;
    }
}
