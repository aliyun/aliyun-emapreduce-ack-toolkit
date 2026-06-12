package com.aliyun.emr.ack.client;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Unit tests for the Server-Sent Events frame parser used by driver-log streaming. No network:
 * the parser is fed a canned SSE byte stream and the handler callbacks are asserted.
 */
public class KyuubiClientSseTest {

    private static KyuubiClient newClient() {
        // Config only supplies auth/base-url, which the parser does not touch; a missing file is fine.
        return new KyuubiClient(new Config("/tmp/__sse_test_nonexistent.conf"));
    }

    private static final class CollectingHandler implements KyuubiClient.DriverLogHandler {
        final List<String> lines = new ArrayList<>();
        final List<Long> timestamps = new ArrayList<>();
        String endReason;
        String errorMessage;

        @Override
        public void onLog(String line, long timestampMillis) {
            lines.add(line);
            timestamps.add(timestampMillis);
        }

        @Override
        public void onEnd(String reason) {
            endReason = reason;
        }

        @Override
        public void onError(String message) {
            errorMessage = message;
        }
    }

    private static KyuubiClient.DriverLogStreamResult parse(String sse, CollectingHandler h)
            throws IOException {
        return newClient().readSseEvents(new BufferedReader(new StringReader(sse)), h);
    }

    @Test
    public void parsesLogEventsHeartbeatsAndEnd() throws IOException {
        String sse =
                "event: log\n"
                + "data: {\"line\":\"hello world\",\"timestamp\":111}\n"
                + "\n"
                + ": keepalive\n"          // comment heartbeat, ignored
                + "\n"
                + "event: log\n"
                + "data: {\"line\":\"second\"}\n"   // no timestamp field -> 0
                + "\n"
                + "event: end\n"
                + "data: {\"reason\":\"pod terminated\"}\n"
                + "\n";
        CollectingHandler h = new CollectingHandler();
        KyuubiClient.DriverLogStreamResult result = parse(sse, h);

        assertEquals(KyuubiClient.DriverLogStreamResult.ENDED, result);
        assertEquals(2, h.lines.size());
        assertEquals("hello world", h.lines.get(0));
        assertEquals(Long.valueOf(111L), h.timestamps.get(0));
        assertEquals("second", h.lines.get(1));
        assertEquals(Long.valueOf(0L), h.timestamps.get(1));
        assertEquals("pod terminated", h.endReason);
        assertNull(h.errorMessage);
    }

    @Test
    public void stripsSingleLeadingSpaceAfterColon() throws IOException {
        // "data: {...}" (with a space) and "data:{...}" (without) must parse identically
        String sse = "event: log\n" + "data:{\"line\":\"tight\"}\n" + "\n"
                + "event: end\n" + "data: {\"reason\":\"done\"}\n" + "\n";
        CollectingHandler h = new CollectingHandler();
        parse(sse, h);
        assertEquals("tight", h.lines.get(0));
    }

    @Test
    public void errorEvent_invokesHandlerAndReportsDisconnected() throws IOException {
        String sse = "event: error\n" + "data: {\"message\":\"boom\"}\n" + "\n";
        CollectingHandler h = new CollectingHandler();
        KyuubiClient.DriverLogStreamResult result = parse(sse, h);
        assertEquals(KyuubiClient.DriverLogStreamResult.DISCONNECTED, result);
        assertEquals("boom", h.errorMessage);
        assertEquals(0, h.lines.size());
    }

    @Test
    public void commentsOnlyThenEof_reportsDisconnectedWithNoEvents() throws IOException {
        String sse = ": waiting\n" + "\n" + ": keepalive\n" + "\n";
        CollectingHandler h = new CollectingHandler();
        KyuubiClient.DriverLogStreamResult result = parse(sse, h);
        assertEquals(KyuubiClient.DriverLogStreamResult.DISCONNECTED, result);
        assertEquals(0, h.lines.size());
        assertNull(h.endReason);
    }

    @Test
    public void malformedJsonData_isSurfacedRawRatherThanDropped() throws IOException {
        String sse = "event: log\n" + "data: not-json\n" + "\n";
        CollectingHandler h = new CollectingHandler();
        parse(sse, h);
        assertEquals(1, h.lines.size());
        assertEquals("not-json", h.lines.get(0));
    }
}
