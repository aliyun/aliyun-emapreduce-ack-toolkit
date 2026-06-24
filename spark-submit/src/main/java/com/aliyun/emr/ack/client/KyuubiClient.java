package com.aliyun.emr.ack.client;

import com.aliyun.emr.ack.cli.SparkSubmitArgs;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

/** Kyuubi REST API client */
public class KyuubiClient {
    private final Config config;
    private final CloseableHttpClient httpClient;
    private final Gson gson;

    private static final int CONNECT_TIMEOUT_MS = 30 * 1000; // 30 seconds
    private static final int SOCKET_TIMEOUT_MS = 5 * 60 * 1000; // 5 minutes
    // A driver-log SSE connection is long-lived: the server sends heartbeat comments every ~15s, so
    // anything past this window of total silence means the connection is dead rather than just
    // idle.
    private static final int DRIVER_LOG_STREAM_SOCKET_TIMEOUT_MS = 60 * 1000; // 1 minute

    public KyuubiClient(Config config) {
        this.config = config;
        RequestConfig requestConfig =
                RequestConfig.custom()
                        .setConnectTimeout(CONNECT_TIMEOUT_MS)
                        .setSocketTimeout(SOCKET_TIMEOUT_MS)
                        .setConnectionRequestTimeout(CONNECT_TIMEOUT_MS)
                        .build();
        this.httpClient =
                HttpClients.custom()
                        .setDefaultRequestConfig(requestConfig)
                        // Disable HttpClient's built-in retries so the application-level Retry
                        // is the single, predictable source of retries for the submission chain.
                        .setRetryHandler(new DefaultHttpRequestRetryHandler(0, false))
                        .build();
        this.gson = new Gson();
    }

    private String getAuthHeader() {
        String auth = config.getUsername() + ":" + config.getPassword();
        byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encodedAuth);
    }

    /**
     * Whether a conf key is for the client only (e.g. retry tuning) and must NOT be forwarded to
     * Kyuubi/Spark as a batch or session config.
     */
    public static boolean isClientOnlyConf(String key) {
        return key != null
                && (key.startsWith("spark.submit.retry.")
                        || key.startsWith("spark.submit.driver.log."));
    }

    /** Submit a batch job to Kyuubi */
    public BatchResponse submitBatch(SparkSubmitArgs args) throws IOException {
        String url = config.getBaseUrl() + "/batches";

        JsonObject batchRequest = new JsonObject();
        batchRequest.addProperty("batchType", args.getBatchType());
        if (args.getClassName() != null && !args.getClassName().isEmpty()) {
            batchRequest.addProperty("className", args.getClassName());
        }
        // Resource is optional for built-in classes (e.g., SparkSQLCLIDriver)
        if (args.getResource() != null && !args.getResource().isEmpty()) {
            batchRequest.addProperty("resource", args.getResource());
        }
        if (args.getProxyUser() != null && !args.getProxyUser().isEmpty()) {
            batchRequest.addProperty("proxyUser", args.getProxyUser());
        }
        if (args.getQueue() != null && !args.getQueue().isEmpty()) {
            batchRequest.addProperty("queue", args.getQueue());
        }

        if (args.getName() != null && !args.getName().isEmpty()) {
            batchRequest.addProperty("name", args.getName());
        } else {
            batchRequest.addProperty("name", "spark-submit-job");
        }

        // Add configuration (ensure submitted-by label and proxy user)
        JsonObject conf = new JsonObject();
        if (!args.getConf().isEmpty()) {
            for (Map.Entry<String, String> entry : args.getConf().entrySet()) {
                if (isClientOnlyConf(entry.getKey())) {
                    continue; // client-only (e.g. retry tuning), do not leak into Spark/Kyuubi
                }
                conf.addProperty(entry.getKey(), entry.getValue());
            }
        }
        if (!conf.has("spark.kubernetes.driver.label.submitted-by")) {
            conf.addProperty("spark.kubernetes.driver.label.submitted-by", "spark-submit");
        }
        // Add proxy user configuration for Kyuubi
        if (args.getProxyUser() != null && !args.getProxyUser().isEmpty()) {
            conf.addProperty("hive.server2.proxy.user", args.getProxyUser());
        }
        batchRequest.add("conf", conf);

        if (!args.getPyFiles().isEmpty()) {
            batchRequest.add("pyFiles", gson.toJsonTree(args.getPyFiles()));
        }
        if (!args.getFiles().isEmpty()) {
            batchRequest.add("files", gson.toJsonTree(args.getFiles()));
        }
        if (!args.getJars().isEmpty()) {
            batchRequest.add("jars", gson.toJsonTree(args.getJars()));
        }
        if (!args.getArchives().isEmpty()) {
            batchRequest.add("archives", gson.toJsonTree(args.getArchives()));
        }
        if (!args.getPackages().isEmpty()) {
            batchRequest.add("packages", gson.toJsonTree(args.getPackages()));
        }
        if (!args.getRepositories().isEmpty()) {
            batchRequest.add("repositories", gson.toJsonTree(args.getRepositories()));
        }

        // Add arguments
        if (!args.getArgs().isEmpty()) {
            batchRequest.add("args", gson.toJsonTree(args.getArgs()));
        }

        HttpPost post = new HttpPost(url);
        post.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
        post.setHeader(HttpHeaders.AUTHORIZATION, getAuthHeader());

        String jsonBody = gson.toJson(batchRequest);
        post.setEntity(new StringEntity(jsonBody, StandardCharsets.UTF_8));

        try (CloseableHttpResponse response = httpClient.execute(post)) {
            HttpEntity entity = response.getEntity();
            String responseBody = EntityUtils.toString(entity, StandardCharsets.UTF_8);

            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode >= 200 && statusCode < 300) {
                return gson.fromJson(responseBody, BatchResponse.class);
            } else {
                throw new HttpStatusException(
                        statusCode,
                        "Failed to submit batch: "
                                + response.getStatusLine()
                                + ", response: "
                                + responseBody);
            }
        }
    }

    /** Get batch status */
    public BatchResponse getBatch(String batchId) throws IOException {
        String url = config.getBaseUrl() + "/batches/" + batchId;

        HttpGet get = new HttpGet(url);
        get.setHeader(HttpHeaders.AUTHORIZATION, getAuthHeader());

        try (CloseableHttpResponse response = httpClient.execute(get)) {
            HttpEntity entity = response.getEntity();
            String responseBody = EntityUtils.toString(entity, StandardCharsets.UTF_8);

            if (response.getStatusLine().getStatusCode() >= 200
                    && response.getStatusLine().getStatusCode() < 300) {
                return gson.fromJson(responseBody, BatchResponse.class);
            } else {
                throw new IOException(
                        "Failed to get batch: "
                                + response.getStatusLine()
                                + ", response: "
                                + responseBody);
            }
        }
    }

    /** Get batch logs */
    public LogResponse getBatchLogs(String batchId, int from, int size) throws IOException {
        String url =
                config.getBaseUrl()
                        + "/batches/"
                        + batchId
                        + "/localLog?from="
                        + from
                        + "&size="
                        + size;

        HttpGet get = new HttpGet(url);
        get.setHeader(HttpHeaders.AUTHORIZATION, getAuthHeader());

        try (CloseableHttpResponse response = httpClient.execute(get)) {
            HttpEntity entity = response.getEntity();
            String responseBody = EntityUtils.toString(entity, StandardCharsets.UTF_8);

            if (response.getStatusLine().getStatusCode() >= 200
                    && response.getStatusLine().getStatusCode() < 300) {
                return gson.fromJson(responseBody, LogResponse.class);
            } else {
                throw new IOException(
                        "Failed to get batch logs: "
                                + response.getStatusLine()
                                + ", response: "
                                + responseBody);
            }
        }
    }

    // =============================================
    // Driver log streaming (Server-Sent Events)
    // =============================================

    /** The outcome of one {@link #streamDriverLog} connection attempt. */
    public enum DriverLogStreamResult {
        /** The server signalled an {@code end} event: this driver-log connection is complete. */
        ENDED,
        /**
         * The connection dropped or errored before an {@code end} event; the caller may reconnect.
         */
        DISCONNECTED,
        /**
         * The server returned 404: driver log streaming is disabled (or unsupported) server-side.
         */
        DISABLED
    }

    /** Receives parsed driver-log Server-Sent Events. */
    public interface DriverLogHandler {
        /** A driver log line (already stripped of any container timestamp). */
        void onLog(String line, long timestampMillis);

        /** The stream ended normally with the given reason (e.g. "pod terminated"). */
        void onEnd(String reason);

        /** The server reported a streaming error. */
        void onError(String message);
    }

    /**
     * Open the batch's driver-pod log stream (Server-Sent Events) and pump events to {@code
     * handler} until the stream ends, the connection drops, or the request is aborted. Blocks the
     * calling thread. Driver log streaming is a Kyuubi 1.12+/Kubernetes feature; a 404 response
     * means it is disabled server-side and is reported as {@link DriverLogStreamResult#DISABLED}.
     *
     * @param tailLines number of trailing lines to start from (use 0 on reconnect to avoid
     *     re-dumping the tail; pair it with a small {@code sinceSeconds} lookback)
     * @param sinceSeconds only return lines newer than this many seconds (0 = no lower bound)
     * @param timestamps whether the server should prefix each line with its container timestamp
     * @param onConnected receives an abort callback as soon as the request is created, so a caller
     *     on another thread can abort the in-flight request to stop streaming promptly
     */
    public DriverLogStreamResult streamDriverLog(
            String batchId,
            int tailLines,
            int sinceSeconds,
            boolean timestamps,
            Consumer<Runnable> onConnected,
            DriverLogHandler handler)
            throws IOException {
        String url =
                config.getBaseUrl()
                        + "/batches/"
                        + batchId
                        + "/driverLog/stream"
                        + "?tailLines="
                        + tailLines
                        + "&sinceSeconds="
                        + sinceSeconds
                        + "&timestamps="
                        + timestamps;

        HttpGet get = new HttpGet(url);
        get.setHeader(HttpHeaders.AUTHORIZATION, getAuthHeader());
        get.setHeader(HttpHeaders.ACCEPT, "text/event-stream");
        get.setConfig(
                RequestConfig.custom()
                        .setConnectTimeout(CONNECT_TIMEOUT_MS)
                        .setConnectionRequestTimeout(CONNECT_TIMEOUT_MS)
                        .setSocketTimeout(DRIVER_LOG_STREAM_SOCKET_TIMEOUT_MS)
                        .build());

        if (onConnected != null) {
            onConnected.accept(get::abort);
        }

        try (CloseableHttpResponse response = httpClient.execute(get)) {
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 404) {
                return DriverLogStreamResult.DISABLED;
            }
            if (statusCode < 200 || statusCode >= 300) {
                HttpEntity entity = response.getEntity();
                String body =
                        entity != null ? EntityUtils.toString(entity, StandardCharsets.UTF_8) : "";
                handler.onError("driver log stream returned HTTP " + statusCode + ": " + body);
                return DriverLogStreamResult.DISCONNECTED;
            }
            HttpEntity entity = response.getEntity();
            if (entity == null) {
                return DriverLogStreamResult.DISCONNECTED;
            }
            try (BufferedReader reader =
                    new BufferedReader(
                            new InputStreamReader(entity.getContent(), StandardCharsets.UTF_8))) {
                return readSseEvents(reader, handler);
            }
        } catch (SocketTimeoutException e) {
            return DriverLogStreamResult.DISCONNECTED;
        } catch (IOException e) {
            if (get.isAborted()) {
                return DriverLogStreamResult
                        .DISCONNECTED; // we aborted on stop(), not a real failure
            }
            throw e;
        }
    }

    /**
     * Parse the SSE byte stream frame by frame until a terminal event or EOF. Visible for testing.
     */
    DriverLogStreamResult readSseEvents(BufferedReader reader, DriverLogHandler handler)
            throws IOException {
        String eventName = null;
        StringBuilder data = new StringBuilder();
        String rawLine;
        while ((rawLine = reader.readLine()) != null) {
            if (rawLine.isEmpty()) {
                // a blank line terminates the current event
                if (eventName != null || data.length() > 0) {
                    DriverLogStreamResult terminal =
                            dispatchSseEvent(eventName, data.toString(), handler);
                    if (terminal != null) {
                        return terminal;
                    }
                }
                eventName = null;
                data.setLength(0);
                continue;
            }
            if (rawLine.charAt(0) == ':') {
                continue; // comment line (heartbeat keep-alive)
            }
            int colon = rawLine.indexOf(':');
            String field = colon >= 0 ? rawLine.substring(0, colon) : rawLine;
            String value = colon >= 0 ? rawLine.substring(colon + 1) : "";
            if (value.startsWith(" ")) {
                value = value.substring(1); // SSE strips a single leading space after the colon
            }
            if ("event".equals(field)) {
                eventName = value;
            } else if ("data".equals(field)) {
                if (data.length() > 0) {
                    data.append('\n');
                }
                data.append(value);
            }
            // id/retry and unknown fields are ignored
        }
        return DriverLogStreamResult.DISCONNECTED; // stream closed without an explicit end event
    }

    /**
     * @return a terminal result when the event ends the stream, otherwise null to keep reading.
     */
    private DriverLogStreamResult dispatchSseEvent(
            String eventName, String data, DriverLogHandler handler) {
        if ("log".equals(eventName)) {
            try {
                JsonObject obj = gson.fromJson(data, JsonObject.class);
                String line = obj != null && obj.has("line") ? obj.get("line").getAsString() : data;
                long ts =
                        obj != null && obj.has("timestamp") ? obj.get("timestamp").getAsLong() : 0L;
                handler.onLog(line, ts);
            } catch (RuntimeException e) {
                handler.onLog(
                        data, 0L); // be forgiving: surface the raw payload rather than dropping it
            }
            return null;
        }
        if ("end".equals(eventName)) {
            handler.onEnd(extractJsonString(data, "reason", "end"));
            return DriverLogStreamResult.ENDED;
        }
        if ("error".equals(eventName)) {
            handler.onError(extractJsonString(data, "message", data));
            return DriverLogStreamResult.DISCONNECTED;
        }
        return null; // unnamed/unknown event — ignore
    }

    private String extractJsonString(String data, String key, String fallback) {
        try {
            JsonObject obj = gson.fromJson(data, JsonObject.class);
            if (obj != null && obj.has(key)) {
                return obj.get(key).getAsString();
            }
        } catch (RuntimeException e) {
            // fall through to the fallback
        }
        return fallback;
    }

    /** Kill a batch job */
    public void killBatch(String batchId) throws IOException {
        String url = config.getBaseUrl() + "/batches/" + batchId;

        HttpDelete delete = new HttpDelete(url);
        delete.setHeader(HttpHeaders.AUTHORIZATION, getAuthHeader());

        try (CloseableHttpResponse response = httpClient.execute(delete)) {
            if (response.getStatusLine().getStatusCode() < 200
                    || response.getStatusLine().getStatusCode() >= 300) {
                HttpEntity entity = response.getEntity();
                String responseBody =
                        entity != null ? EntityUtils.toString(entity, StandardCharsets.UTF_8) : "";
                throw new IOException(
                        "Failed to kill batch: "
                                + response.getStatusLine()
                                + ", response: "
                                + responseBody);
            }
        }
    }

    /**
     * Upload a file to Kyuubi server (requires kyuubi-upload-plugin). The server uploads the file
     * to the configured staging path (e.g., OSS) and returns the remote URI.
     *
     * @return the remote URI (e.g., oss://bucket/.../query.sql)
     * @throws IOException on network error or non-2xx response
     */
    public String uploadFile(byte[] content, String fileName) throws IOException {
        String url = config.getBaseUrl() + "/files/upload";

        String boundary = "----SparkSubmitBoundary" + System.currentTimeMillis();

        ByteArrayOutputStream body = new ByteArrayOutputStream();
        byte[] header =
                ("--"
                                + boundary
                                + "\r\n"
                                + "Content-Disposition: form-data; name=\"file\"; filename=\""
                                + fileName
                                + "\"\r\n"
                                + "Content-Type: application/octet-stream\r\n"
                                + "\r\n")
                        .getBytes(StandardCharsets.UTF_8);
        body.write(header);
        body.write(content);
        byte[] footer = ("\r\n--" + boundary + "--\r\n").getBytes(StandardCharsets.UTF_8);
        body.write(footer);

        HttpPost post = new HttpPost(url);
        post.setHeader(HttpHeaders.CONTENT_TYPE, "multipart/form-data; boundary=" + boundary);
        post.setHeader(HttpHeaders.AUTHORIZATION, getAuthHeader());
        post.setEntity(new ByteArrayEntity(body.toByteArray()));

        try (CloseableHttpResponse response = httpClient.execute(post)) {
            int statusCode = response.getStatusLine().getStatusCode();
            HttpEntity entity = response.getEntity();
            String responseBody =
                    entity != null ? EntityUtils.toString(entity, StandardCharsets.UTF_8) : "";

            if (statusCode >= 200 && statusCode < 300) {
                JsonObject json = gson.fromJson(responseBody, JsonObject.class);
                if (json != null && json.has("uri")) {
                    return json.get("uri").getAsString();
                }
                throw new IOException(
                        "Upload succeeded but response missing 'uri': " + responseBody);
            } else {
                throw new HttpStatusException(
                        statusCode,
                        "Failed to upload file (HTTP "
                                + statusCode
                                + "): "
                                + response.getStatusLine()
                                + ", response: "
                                + responseBody);
            }
        }
    }

    // =============================================
    // Session & Operation API (for spark-sql mode)
    // =============================================

    /** Create a new session */
    public SessionResponse createSession(Map<String, String> configs) throws IOException {
        String url = config.getBaseUrl() + "/sessions";

        JsonObject requestBody = new JsonObject();
        if (configs != null && !configs.isEmpty()) {
            JsonObject confObj = new JsonObject();
            for (Map.Entry<String, String> entry : configs.entrySet()) {
                if (isClientOnlyConf(entry.getKey())) {
                    continue; // client-only (e.g. retry tuning), do not leak into session configs
                }
                confObj.addProperty(entry.getKey(), entry.getValue());
            }
            requestBody.add("configs", confObj);
        }

        HttpPost post = new HttpPost(url);
        post.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
        post.setHeader(HttpHeaders.AUTHORIZATION, getAuthHeader());
        post.setEntity(new StringEntity(gson.toJson(requestBody), StandardCharsets.UTF_8));

        try (CloseableHttpResponse response = httpClient.execute(post)) {
            HttpEntity entity = response.getEntity();
            String responseBody = EntityUtils.toString(entity, StandardCharsets.UTF_8);

            if (response.getStatusLine().getStatusCode() >= 200
                    && response.getStatusLine().getStatusCode() < 300) {
                return gson.fromJson(responseBody, SessionResponse.class);
            } else {
                throw new IOException(
                        "Failed to create session: "
                                + response.getStatusLine()
                                + ", response: "
                                + responseBody);
            }
        }
    }

    /** Close a session */
    public void closeSession(String sessionHandle) throws IOException {
        String url = config.getBaseUrl() + "/sessions/" + sessionHandle;

        HttpDelete delete = new HttpDelete(url);
        delete.setHeader(HttpHeaders.AUTHORIZATION, getAuthHeader());

        try (CloseableHttpResponse response = httpClient.execute(delete)) {
            if (response.getStatusLine().getStatusCode() < 200
                    || response.getStatusLine().getStatusCode() >= 300) {
                HttpEntity entity = response.getEntity();
                String responseBody =
                        entity != null ? EntityUtils.toString(entity, StandardCharsets.UTF_8) : "";
                throw new IOException(
                        "Failed to close session: "
                                + response.getStatusLine()
                                + ", response: "
                                + responseBody);
            }
        }
    }

    /** Execute a SQL statement in a session */
    public OperationResponse executeStatement(
            String sessionHandle, String statement, boolean runAsync) throws IOException {
        String url = config.getBaseUrl() + "/sessions/" + sessionHandle + "/operations/statement";

        JsonObject requestBody = new JsonObject();
        requestBody.addProperty("statement", statement);
        requestBody.addProperty("runAsync", runAsync);

        HttpPost post = new HttpPost(url);
        post.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
        post.setHeader(HttpHeaders.AUTHORIZATION, getAuthHeader());
        post.setEntity(new StringEntity(gson.toJson(requestBody), StandardCharsets.UTF_8));

        try (CloseableHttpResponse response = httpClient.execute(post)) {
            HttpEntity entity = response.getEntity();
            String responseBody = EntityUtils.toString(entity, StandardCharsets.UTF_8);

            if (response.getStatusLine().getStatusCode() >= 200
                    && response.getStatusLine().getStatusCode() < 300) {
                return gson.fromJson(responseBody, OperationResponse.class);
            } else {
                throw new IOException(
                        "Failed to execute statement: "
                                + response.getStatusLine()
                                + ", response: "
                                + responseBody);
            }
        }
    }

    /** Get operation event (status) */
    public OperationEvent getOperationEvent(String operationHandle) throws IOException {
        String url = config.getBaseUrl() + "/operations/" + operationHandle + "/event";

        HttpGet get = new HttpGet(url);
        get.setHeader(HttpHeaders.AUTHORIZATION, getAuthHeader());

        try (CloseableHttpResponse response = httpClient.execute(get)) {
            HttpEntity entity = response.getEntity();
            String responseBody = EntityUtils.toString(entity, StandardCharsets.UTF_8);

            if (response.getStatusLine().getStatusCode() >= 200
                    && response.getStatusLine().getStatusCode() < 300) {
                return gson.fromJson(responseBody, OperationEvent.class);
            } else {
                throw new IOException(
                        "Failed to get operation event: "
                                + response.getStatusLine()
                                + ", response: "
                                + responseBody);
            }
        }
    }

    /** Get operation result set metadata (column descriptions) */
    public ResultSetMetadata getResultSetMetadata(String operationHandle) throws IOException {
        String url = config.getBaseUrl() + "/operations/" + operationHandle + "/resultsetmetadata";

        HttpGet get = new HttpGet(url);
        get.setHeader(HttpHeaders.AUTHORIZATION, getAuthHeader());

        try (CloseableHttpResponse response = httpClient.execute(get)) {
            HttpEntity entity = response.getEntity();
            String responseBody = EntityUtils.toString(entity, StandardCharsets.UTF_8);

            if (response.getStatusLine().getStatusCode() >= 200
                    && response.getStatusLine().getStatusCode() < 300) {
                return gson.fromJson(responseBody, ResultSetMetadata.class);
            } else {
                throw new IOException(
                        "Failed to get result set metadata: "
                                + response.getStatusLine()
                                + ", response: "
                                + responseBody);
            }
        }
    }

    /** Get operation result row set */
    public RowSetResponse getOperationRowSet(
            String operationHandle, int maxRows, String fetchOrientation) throws IOException {
        String url =
                config.getBaseUrl()
                        + "/operations/"
                        + operationHandle
                        + "/rowset?maxrows="
                        + maxRows
                        + "&fetchorientation="
                        + fetchOrientation;

        HttpGet get = new HttpGet(url);
        get.setHeader(HttpHeaders.AUTHORIZATION, getAuthHeader());

        try (CloseableHttpResponse response = httpClient.execute(get)) {
            HttpEntity entity = response.getEntity();
            String responseBody = EntityUtils.toString(entity, StandardCharsets.UTF_8);

            if (response.getStatusLine().getStatusCode() >= 200
                    && response.getStatusLine().getStatusCode() < 300) {
                return gson.fromJson(responseBody, RowSetResponse.class);
            } else {
                throw new IOException(
                        "Failed to get operation row set: "
                                + response.getStatusLine()
                                + ", response: "
                                + responseBody);
            }
        }
    }

    /** Get operation log lines */
    public LogResponse getOperationLog(String operationHandle, int maxRows) throws IOException {
        String url =
                config.getBaseUrl() + "/operations/" + operationHandle + "/log?maxrows=" + maxRows;

        HttpGet get = new HttpGet(url);
        get.setHeader(HttpHeaders.AUTHORIZATION, getAuthHeader());

        try (CloseableHttpResponse response = httpClient.execute(get)) {
            HttpEntity entity = response.getEntity();
            String responseBody = EntityUtils.toString(entity, StandardCharsets.UTF_8);

            if (response.getStatusLine().getStatusCode() >= 200
                    && response.getStatusLine().getStatusCode() < 300) {
                return gson.fromJson(responseBody, LogResponse.class);
            } else {
                throw new IOException(
                        "Failed to get operation log: "
                                + response.getStatusLine()
                                + ", response: "
                                + responseBody);
            }
        }
    }

    /**
     * Cancel or close an operation
     *
     * @param action "cancel" or "close"
     */
    public void updateOperation(String operationHandle, String action) throws IOException {
        String url = config.getBaseUrl() + "/operations/" + operationHandle;

        JsonObject requestBody = new JsonObject();
        requestBody.addProperty("action", action);

        org.apache.http.client.methods.HttpPut put =
                new org.apache.http.client.methods.HttpPut(url);
        put.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
        put.setHeader(HttpHeaders.AUTHORIZATION, getAuthHeader());
        put.setEntity(new StringEntity(gson.toJson(requestBody), StandardCharsets.UTF_8));

        try (CloseableHttpResponse response = httpClient.execute(put)) {
            if (response.getStatusLine().getStatusCode() < 200
                    || response.getStatusLine().getStatusCode() >= 300) {
                HttpEntity entity = response.getEntity();
                String responseBody =
                        entity != null ? EntityUtils.toString(entity, StandardCharsets.UTF_8) : "";
                throw new IOException(
                        "Failed to "
                                + action
                                + " operation: "
                                + response.getStatusLine()
                                + ", response: "
                                + responseBody);
            }
        }
    }

    public void close() throws IOException {
        httpClient.close();
    }

    /** Batch response model */
    @Getter
    @Setter
    public static class BatchResponse {
        private String id;
        private String user;
        private String batchType;
        private String name;
        private Long appStartTime;
        private String appId;
        private String appUrl;
        private String appState;
        private String appDiagnostic;
        private String kyuubiInstance;
        private String state;
        private Long createTime;
        private Long endTime;

        public boolean isFinished() {
            return "FINISHED".equals(state) || "ERROR".equals(state) || "CANCELED".equals(state);
        }
    }

    /** Log response model */
    @Getter
    @Setter
    public static class LogResponse {
        private java.util.List<String> logRowSet;
        private Integer rowCount;
    }

    /** Session response model */
    @Getter
    @Setter
    public static class SessionResponse {
        private String identifier;
        private String kyuubiInstance;
    }

    /** Operation response model (for executeStatement) */
    @Getter
    @Setter
    public static class OperationResponse {
        private String identifier;
    }

    /** Operation event model */
    @Getter
    public static class OperationEvent {
        private String statementId;
        private String remoteId;
        private String statement;
        private Boolean shouldRunAsync;
        private String state;
        private Long eventTime;
        private Long createTime;
        private Long startTime;
        private Long completeTime;
        private String exception;
        private String sessionId;
        private String sessionUser;

        public boolean isTerminal() {
            if (state == null) return false;
            // Kyuubi may return states with or without _STATE suffix
            String s = state.replace("_STATE", "");
            return "FINISHED".equals(s)
                    || "ERROR".equals(s)
                    || "CANCELED".equals(s)
                    || "CLOSED".equals(s)
                    || "TIMEOUT".equals(s);
        }
    }

    /** Result set metadata model */
    @Getter
    @Setter
    public static class ResultSetMetadata {
        private List<ColumnDesc> columns;
    }

    /** Column description model */
    @Getter
    public static class ColumnDesc {
        private String columnName;
        private String dataType;
        private Integer columnIndex;
        private Integer precision;
        private Integer scale;
        private String comment;
    }

    /** Row set response model */
    @Getter
    @Setter
    public static class RowSetResponse {
        private List<Row> rows;
        private Integer rowCount;
    }

    /** Row model */
    @Getter
    @Setter
    public static class Row {
        private List<Field> fields;
    }

    /** Field model */
    @Getter
    public static class Field {
        private String dataType;
        private Object value;
    }
}
