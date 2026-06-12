package com.aliyun.emr.ack.command;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.aliyun.emr.ack.cli.SparkSubmitArgs;
import com.aliyun.emr.ack.client.Config;
import com.aliyun.emr.ack.client.HttpStatusException;
import com.aliyun.emr.ack.client.KyuubiClient;
import com.aliyun.emr.ack.client.Retry;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SqlUploaderTest {

    @Rule public TemporaryFolder tmp = new TemporaryFolder();

    private HttpServer server;
    private String uploadedBody;

    @Before
    public void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.start();
    }

    @After
    public void stopServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    public void upload_usesKyuubiServerUploadWhenPluginReturnsUri() throws Exception {
        respond("/api/v1/files/upload", 200, "{\"uri\":\"oss://bucket/staged/query.sql\"}");

        String uri =
                SqlUploader.upload(
                        newClient(),
                        "SELECT 1".getBytes(StandardCharsets.UTF_8),
                        new HashMap<String, String>(),
                        newConfig(),
                        noRetryConfig());

        assertEquals("oss://bucket/staged/query.sql", uri);
        assertTrue(uploadedBody.contains("filename=\"query.sql\""));
        assertTrue(uploadedBody.contains("SELECT 1"));
    }

    @Test
    public void upload_nonPluginServerErrorDoesNotFallBackToOss() throws Exception {
        respond("/api/v1/files/upload", 500, "{\"error\":\"server down\"}");

        try {
            SqlUploader.upload(
                    newClient(),
                    "SELECT 1".getBytes(StandardCharsets.UTF_8),
                    new HashMap<String, String>(),
                    newConfig(),
                    noRetryConfig());
            fail("expected HttpStatusException");
        } catch (HttpStatusException e) {
            assertEquals(500, e.getStatusCode());
            assertTrue(e.getMessage().contains("server down"));
        }
    }

    private KyuubiClient newClient() throws IOException {
        return new KyuubiClient(newConfig());
    }

    private Config newConfig() throws IOException {
        Config config = new Config(tmp.getRoot().getAbsolutePath() + "/missing.conf");
        SparkSubmitArgs args = new SparkSubmitArgs();
        args.setKyuubiUrl("http://127.0.0.1:" + server.getAddress().getPort());
        args.setKyuubiUser("e2e");
        config.applyOverrides(args);
        return config;
    }

    private static Retry.RetryConfig noRetryConfig() {
        return new Retry.RetryConfig(1, 0L, 0L, 1.0, Retry::isTransientNetwork);
    }

    private void respond(String path, int status, String body) {
        server.createContext(
                path,
                new HttpHandler() {
                    @Override
                    public void handle(HttpExchange exchange) throws IOException {
                        uploadedBody = read(exchange.getRequestBody());
                        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
                        exchange.getResponseHeaders()
                                .put("Content-Type", Collections.singletonList("application/json"));
                        exchange.sendResponseHeaders(status, bytes.length);
                        try (OutputStream out = exchange.getResponseBody()) {
                            out.write(bytes);
                        }
                    }
                });
    }

    private static String read(InputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buf = new byte[4096];
        int n;
        while ((n = in.read(buf)) >= 0) {
            out.write(buf, 0, n);
        }
        return new String(out.toByteArray(), StandardCharsets.UTF_8);
    }
}
