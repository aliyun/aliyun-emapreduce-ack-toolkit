package com.aliyun.emr.ack;

import org.apache.http.NoHttpResponseException;
import org.apache.http.conn.ConnectTimeoutException;
import org.junit.Test;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RetryTest {

    // ---- isConnectPhaseOnly: only connection-establishment failures ----

    @Test
    public void connectPhaseOnly_matchesConnectEstablishmentErrors() {
        assertTrue(Retry.isConnectPhaseOnly(new ConnectException("refused")));
        assertTrue(Retry.isConnectPhaseOnly(new ConnectTimeoutException("connect timeout")));
        assertTrue(Retry.isConnectPhaseOnly(new UnknownHostException("dns")));
    }

    @Test
    public void connectPhaseOnly_rejectsAmbiguousAndResponseErrors() {
        // Read timeout: request may already have been sent -> must NOT retry submit.
        assertFalse(Retry.isConnectPhaseOnly(new SocketTimeoutException("read timeout")));
        assertFalse(Retry.isConnectPhaseOnly(new NoHttpResponseException("no response")));
        assertFalse(Retry.isConnectPhaseOnly(new HttpStatusException(429, "rate limited")));
        assertFalse(Retry.isConnectPhaseOnly(new HttpStatusException(503, "unavailable")));
        assertFalse(Retry.isConnectPhaseOnly(new HttpStatusException(500, "server error")));
    }

    // ---- isTransientNetwork: transient errors + 5xx/429 for idempotent ops ----

    @Test
    public void transientNetwork_matchesTransientErrors() {
        assertTrue(Retry.isTransientNetwork(new ConnectException("refused")));
        assertTrue(Retry.isTransientNetwork(new ConnectTimeoutException("connect timeout")));
        assertTrue(Retry.isTransientNetwork(new UnknownHostException("dns")));
        assertTrue(Retry.isTransientNetwork(new SocketTimeoutException("read timeout")));
        assertTrue(Retry.isTransientNetwork(new NoHttpResponseException("no response")));
        assertTrue(Retry.isTransientNetwork(new SSLException("ssl reset")));
        assertTrue(Retry.isTransientNetwork(new HttpStatusException(500, "server error")));
        assertTrue(Retry.isTransientNetwork(new HttpStatusException(503, "unavailable")));
        assertTrue(Retry.isTransientNetwork(new HttpStatusException(429, "rate limited")));
    }

    @Test
    public void transientNetwork_rejectsPermanentErrors() {
        // Handshake failures (bad cert / protocol) are stable -> not retryable.
        assertFalse(Retry.isTransientNetwork(new SSLHandshakeException("bad cert")));
        assertFalse(Retry.isTransientNetwork(new HttpStatusException(400, "bad request")));
        assertFalse(Retry.isTransientNetwork(new HttpStatusException(401, "unauthorized")));
        assertFalse(Retry.isTransientNetwork(new HttpStatusException(404, "not found")));
        assertFalse(Retry.isTransientNetwork(new HttpStatusException(409, "conflict")));
        assertFalse(Retry.isTransientNetwork(new IOException("generic")));
    }

    @Test
    public void retryInterrupted_isNotSwallowedByAnyPredicate() {
        Retry.RetryInterruptedException ri = new Retry.RetryInterruptedException("interrupted");
        assertFalse(Retry.isTransientNetwork(ri));
        assertFalse(Retry.isConnectPhaseOnly(ri));
    }

    // ---- RetryConfig clamping ----

    @Test
    public void retryConfig_clampsIllegalValues() {
        Retry.RetryConfig c = new Retry.RetryConfig(-5, -100L, -1L, 0.5, Retry::isTransientNetwork);
        assertEquals(1, c.maxAttempts);            // max(1, -5)
        assertEquals(0L, c.initialBackoffMs);      // max(0, -100)
        assertEquals(0L, c.maxBackoffMs);          // max(initial=0, -1)
        assertEquals(1.0, c.multiplier, 1e-9);     // 0.5 -> 1.0
    }

    @Test
    public void retryConfig_keepsValidValues() {
        Retry.RetryConfig c = new Retry.RetryConfig(3, 1000L, 8000L, 2.0, Retry::isTransientNetwork);
        assertEquals(3, c.maxAttempts);
        assertEquals(1000L, c.initialBackoffMs);
        assertEquals(8000L, c.maxBackoffMs);
        assertEquals(2.0, c.multiplier, 1e-9);
    }

    // ---- computeBackoffWithJitter: full jitter within bounds ----

    @Test
    public void backoff_isWithinFullJitterBounds() {
        Retry.RetryConfig c = new Retry.RetryConfig(6, 1000L, 8000L, 2.0, Retry::isTransientNetwork);
        for (int attempt = 1; attempt <= 6; attempt++) {
            double expected = 1000.0 * Math.pow(2.0, attempt - 1);
            long cap = (long) Math.min(8000.0, expected);
            for (int i = 0; i < 300; i++) {
                long v = Retry.computeBackoffWithJitter(c, attempt);
                assertTrue("attempt=" + attempt + " v=" + v + " cap=" + cap, v >= 0 && v <= cap);
            }
        }
    }

    @Test
    public void backoff_isZeroWhenInitialIsZero() {
        Retry.RetryConfig c = new Retry.RetryConfig(3, 0L, 0L, 2.0, Retry::isTransientNetwork);
        assertEquals(0L, Retry.computeBackoffWithJitter(c, 1));
        assertEquals(0L, Retry.computeBackoffWithJitter(c, 5));
    }

    // ---- execute: retry counts + immediate rethrow ----

    @Test
    public void execute_retriesTransientThenSucceeds() throws IOException {
        // zero backoff for a fast test
        Retry.RetryConfig c = new Retry.RetryConfig(3, 0L, 0L, 1.0, Retry::isTransientNetwork);
        AtomicInteger calls = new AtomicInteger();
        String result = Retry.execute("test", c, () -> {
            int n = calls.incrementAndGet();
            if (n < 3) {
                throw new SocketTimeoutException("transient " + n);
            }
            return "ok";
        });
        assertEquals("ok", result);
        assertEquals(3, calls.get());
    }

    @Test
    public void execute_doesNotRetryNonRetryable() {
        Retry.RetryConfig c = new Retry.RetryConfig(3, 0L, 0L, 1.0, Retry::isTransientNetwork);
        AtomicInteger calls = new AtomicInteger();
        try {
            Retry.execute("test", c, () -> {
                calls.incrementAndGet();
                throw new HttpStatusException(400, "bad request");
            });
            fail("expected HttpStatusException");
        } catch (HttpStatusException e) {
            assertEquals(400, e.getStatusCode());
        } catch (IOException e) {
            fail("wrong exception: " + e);
        }
        assertEquals(1, calls.get());
    }

    @Test
    public void execute_rethrowsAfterExhaustingAttempts() {
        Retry.RetryConfig c = new Retry.RetryConfig(3, 0L, 0L, 1.0, Retry::isTransientNetwork);
        AtomicInteger calls = new AtomicInteger();
        try {
            Retry.execute("test", c, () -> {
                calls.incrementAndGet();
                throw new SocketTimeoutException("always");
            });
            fail("expected SocketTimeoutException");
        } catch (IOException e) {
            assertTrue(e instanceof SocketTimeoutException);
        }
        assertEquals(3, calls.get());
    }
}
