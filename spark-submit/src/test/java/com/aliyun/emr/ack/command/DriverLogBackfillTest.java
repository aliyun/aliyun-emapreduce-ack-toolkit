package com.aliyun.emr.ack.command;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.aliyun.emr.ack.client.Config;
import com.aliyun.emr.ack.client.KyuubiClient;
import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Regression test for the short-job driver-log gap: a pod that finishes before the background
 * stream attaches leaves {@code seenAnyLine == false}, so {@link BatchMonitor} pulls the
 * now-complete driver log once via {@link DriverLogStreamer#drainFinalBackfill()}. Without that
 * backfill a quick job prints no driver output at all.
 *
 * <p>Drives the real {@link DriverLogStreamer} + {@link KyuubiClient} over loopback against an
 * in-JVM {@link HttpServer} stub (JDK built-in, no extra dependency). A fresh streamer's {@code
 * seenAnyLine == false} is exactly the real "missed the live window" precondition, so the backfill
 * is tested directly — no background thread, no timing race.
 */
public class DriverLogBackfillTest {

    private static final String BATCH_ID = "batch-xyz";
    private static final String SPARK_VERSION_LINE =
            "26/06/12 08:16:22 INFO SparkContext: Running Spark version 3.5.7";
    private static final String PI_LINE = "Pi is roughly 3.14159";

    private HttpServer server;
    private File configFile;
    private PrintStream originalOut;
    private ByteArrayOutputStream captured;

    @Before
    public void setUp() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        // The finished pod's complete driver log, served on every stream request.
        server.createContext(
                "/api/v1/batches/" + BATCH_ID + "/driverLog/stream",
                exchange -> respond(exchange, fullLogSse()));
        server.start();

        configFile = File.createTempFile("driver-log-backfill", ".conf");
        configFile.deleteOnExit();
        try (PrintWriter w = new PrintWriter(configFile, "UTF-8")) {
            w.println("kyuubi.server.url=http://127.0.0.1:" + server.getAddress().getPort());
            w.println("kyuubi.server.username=test");
            w.println("kyuubi.server.password=test");
        }

        originalOut = System.out;
        captured = new ByteArrayOutputStream();
        System.setOut(new PrintStream(captured, true, "UTF-8"));
    }

    @After
    public void tearDown() {
        System.setOut(originalOut);
        if (server != null) {
            server.stop(0);
        }
        if (configFile != null) {
            configFile.delete();
        }
    }

    private static void respond(com.sun.net.httpserver.HttpExchange exchange, String body)
            throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(200, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private static String fullLogSse() {
        return "event: log\n"
                + "data: {\"line\":\""
                + SPARK_VERSION_LINE
                + "\",\"timestamp\":1}\n\n"
                + "event: log\n"
                + "data: {\"line\":\""
                + PI_LINE
                + "\",\"timestamp\":2}\n\n"
                + "event: end\n"
                + "data: {\"reason\":\"pod terminated\"}\n\n";
    }

    private DriverLogStreamer freshStreamer() {
        KyuubiClient client = new KyuubiClient(new Config(configFile.getAbsolutePath()));
        // A just-constructed streamer has seenAnyLine == false: the live window was missed.
        return new DriverLogStreamer(client, BATCH_ID, DriverLogFilter.fromRegexes(null, null));
    }

    @Test
    public void backfillRecoversDriverLogWhenLiveStreamMissedTheShortLivedPod() {
        DriverLogStreamer streamer = freshStreamer();
        assertFalse("precondition: live stream received nothing", streamer.receivedAnyLine());

        streamer.drainFinalBackfill();

        String out = captured.toString();
        assertTrue("backfill prints the non-streaming header", out.contains("=== Driver Log ==="));
        assertTrue("backfill prints the Spark version line", out.contains(SPARK_VERSION_LINE));
        assertTrue("backfill prints the Pi result line", out.contains(PI_LINE));
        assertTrue("backfill is recorded as received", streamer.receivedAnyLine());
    }

    @Test
    public void backfillIsNoOpOnceLinesHaveBeenSeen() {
        DriverLogStreamer streamer = freshStreamer();
        streamer.drainFinalBackfill(); // first call prints the log and flips seenAnyLine
        String afterFirst = captured.toString();
        assertTrue("first backfill printed the log", afterFirst.contains(PI_LINE));

        streamer.drainFinalBackfill(); // second call must not dump the log again
        assertEquals(
                "no extra output from the gated no-op backfill", afterFirst, captured.toString());
    }
}
