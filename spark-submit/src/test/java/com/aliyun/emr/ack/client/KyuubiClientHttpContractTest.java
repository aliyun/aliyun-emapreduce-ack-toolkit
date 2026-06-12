package com.aliyun.emr.ack.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.aliyun.emr.ack.cli.SparkSubmitArgs;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class KyuubiClientHttpContractTest {

    @Rule public TemporaryFolder tmp = new TemporaryFolder();

    private HttpServer server;
    private final Map<String, RecordedRequest> requests = new HashMap<>();

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
    public void submitBatch_sendsExpectedJsonAndFiltersClientOnlyConfs() throws Exception {
        respondJson("/api/v1/batches", 200, "{\"id\":\"b-1\",\"state\":\"PENDING\"}");

        SparkSubmitArgs args = new SparkSubmitArgs();
        args.setBatchType("SPARK");
        args.setClassName("com.example.Main");
        args.setResource("oss://bucket/app.jar");
        args.setName("job-name");
        args.setProxyUser("alice");
        args.setQueue("etl");
        args.getArgs().add("arg1");
        args.getArgs().add("arg2");
        args.getConf().put("spark.executor.instances", "2");
        args.getConf().put("spark.submit.retry.maxAttempts", "9");
        args.getConf().put("spark.submit.driver.log.enabled", "false");
        args.getJars().add("dep.jar");
        args.getFiles().add("conf.txt");
        args.getPackages().add("g:a:1");

        KyuubiClient.BatchResponse response = newClient().submitBatch(args);

        assertEquals("b-1", response.getId());
        RecordedRequest req = requests.get("/api/v1/batches");
        assertEquals("POST", req.method);
        assertEquals("application/json", req.header("Content-type"));
        assertEquals(authHeader(), req.header("Authorization"));

        JsonObject body = JsonParser.parseString(req.body).getAsJsonObject();
        assertEquals("SPARK", body.get("batchType").getAsString());
        assertEquals("com.example.Main", body.get("className").getAsString());
        assertEquals("oss://bucket/app.jar", body.get("resource").getAsString());
        assertEquals("job-name", body.get("name").getAsString());
        assertEquals("alice", body.get("proxyUser").getAsString());
        assertEquals("etl", body.get("queue").getAsString());

        JsonObject conf = body.getAsJsonObject("conf");
        assertEquals("2", conf.get("spark.executor.instances").getAsString());
        assertEquals(
                "spark-submit",
                conf.get("spark.kubernetes.driver.label.submitted-by").getAsString());
        assertEquals("alice", conf.get("hive.server2.proxy.user").getAsString());
        assertFalse(conf.has("spark.submit.retry.maxAttempts"));
        assertFalse(conf.has("spark.submit.driver.log.enabled"));

        assertArray(body.getAsJsonArray("args"), "arg1", "arg2");
        assertArray(body.getAsJsonArray("jars"), "dep.jar");
        assertArray(body.getAsJsonArray("files"), "conf.txt");
        assertArray(body.getAsJsonArray("packages"), "g:a:1");
    }

    @Test
    public void submitBatch_non2xxThrowsTypedStatusException() throws Exception {
        respondJson("/api/v1/batches", 503, "{\"error\":\"busy\"}");

        SparkSubmitArgs args = new SparkSubmitArgs();
        args.setBatchType("SPARK");
        args.setClassName("X");
        args.setResource("x.jar");

        try {
            newClient().submitBatch(args);
            fail("expected HttpStatusException");
        } catch (HttpStatusException e) {
            assertEquals(503, e.getStatusCode());
            assertTrue(e.getMessage().contains("busy"));
        }
    }

    @Test
    public void createSession_filtersClientOnlyConfigs() throws Exception {
        respondJson("/api/v1/sessions", 200, "{\"identifier\":\"s-1\"}");

        Map<String, String> configs = new HashMap<>();
        configs.put("spark.sql.shuffle.partitions", "4");
        configs.put("spark.submit.retry.enabled", "false");
        configs.put("spark.submit.driver.log.grep", "WARN");

        KyuubiClient.SessionResponse response = newClient().createSession(configs);

        assertEquals("s-1", response.getIdentifier());
        RecordedRequest req = requests.get("/api/v1/sessions");
        assertEquals("POST", req.method);
        assertEquals(authHeader(), req.header("Authorization"));

        JsonObject body = JsonParser.parseString(req.body).getAsJsonObject();
        JsonObject sentConfigs = body.getAsJsonObject("configs");
        assertEquals("4", sentConfigs.get("spark.sql.shuffle.partitions").getAsString());
        assertFalse(sentConfigs.has("spark.submit.retry.enabled"));
        assertFalse(sentConfigs.has("spark.submit.driver.log.grep"));
    }

    @Test
    public void uploadFile_sendsMultipartAndReturnsUri() throws Exception {
        respondJson("/api/v1/files/upload", 200, "{\"uri\":\"oss://bucket/query.sql\"}");

        String uri =
                newClient().uploadFile("SELECT 1".getBytes(StandardCharsets.UTF_8), "query.sql");

        assertEquals("oss://bucket/query.sql", uri);
        RecordedRequest req = requests.get("/api/v1/files/upload");
        assertEquals("POST", req.method);
        assertEquals(authHeader(), req.header("Authorization"));
        assertTrue(req.header("Content-type").startsWith("multipart/form-data; boundary="));
        assertTrue(req.body.contains("filename=\"query.sql\""));
        assertTrue(req.body.contains("SELECT 1"));
    }

    @Test
    public void uploadFile_successWithoutUriIsRejected() throws Exception {
        respondJson("/api/v1/files/upload", 200, "{\"ok\":true}");

        try {
            newClient().uploadFile("SELECT 1".getBytes(StandardCharsets.UTF_8), "query.sql");
            fail("expected IOException");
        } catch (IOException e) {
            assertTrue(e.getMessage().contains("missing 'uri'"));
        }
    }

    private KyuubiClient newClient() throws IOException {
        Config config = new Config(tmp.getRoot().getAbsolutePath() + "/missing.conf");
        SparkSubmitArgs args = new SparkSubmitArgs();
        args.setKyuubiUrl(baseUrl());
        args.setKyuubiUser("bob");
        args.setKyuubiPassword("secret");
        config.applyOverrides(args);
        return new KyuubiClient(config);
    }

    private String baseUrl() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    private String authHeader() {
        String token =
                Base64.getEncoder().encodeToString("bob:secret".getBytes(StandardCharsets.UTF_8));
        return "Basic " + token;
    }

    private void respondJson(String path, int status, String responseBody) {
        server.createContext(path, new JsonHandler(path, status, responseBody));
    }

    private static void assertArray(JsonArray array, String... expected) {
        assertNotNull(array);
        assertEquals(expected.length, array.size());
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], array.get(i).getAsString());
        }
    }

    private final class JsonHandler implements HttpHandler {
        private final String path;
        private final int status;
        private final String responseBody;

        private JsonHandler(String path, int status, String responseBody) {
            this.path = path;
            this.status = status;
            this.responseBody = responseBody;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            RecordedRequest req =
                    new RecordedRequest(
                            exchange.getRequestMethod(),
                            new HashMap<String, String>(),
                            read(exchange.getRequestBody()));
            for (Map.Entry<String, java.util.List<String>> h :
                    exchange.getRequestHeaders().entrySet()) {
                req.headers.put(
                        h.getKey().toLowerCase(),
                        h.getValue().isEmpty() ? "" : h.getValue().get(0));
            }
            requests.put(path, req);

            byte[] bytes = responseBody.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders()
                    .put("Content-Type", Collections.singletonList("application/json"));
            exchange.sendResponseHeaders(status, bytes.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(bytes);
            }
        }
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

    private static final class RecordedRequest {
        final String method;
        final Map<String, String> headers;
        final String body;

        private RecordedRequest(String method, Map<String, String> headers, String body) {
            this.method = method;
            this.headers = headers;
            this.body = body;
        }

        String header(String name) {
            return headers.get(name.toLowerCase());
        }
    }
}
