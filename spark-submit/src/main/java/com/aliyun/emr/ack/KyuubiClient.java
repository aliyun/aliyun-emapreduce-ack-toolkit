package com.aliyun.emr.ack;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Kyuubi REST API client
 */
public class KyuubiClient {
    private final Config config;
    private final CloseableHttpClient httpClient;
    private final Gson gson;
    
    private static final int CONNECT_TIMEOUT_MS = 30 * 1000; // 30 seconds
    private static final int SOCKET_TIMEOUT_MS = 5 * 60 * 1000; // 5 minutes

    public KyuubiClient(Config config) {
        this.config = config;
        RequestConfig requestConfig = RequestConfig.custom()
            .setConnectTimeout(CONNECT_TIMEOUT_MS)
            .setSocketTimeout(SOCKET_TIMEOUT_MS)
            .setConnectionRequestTimeout(CONNECT_TIMEOUT_MS)
            .build();
        this.httpClient = HttpClients.custom()
            .setDefaultRequestConfig(requestConfig)
            .build();
        this.gson = new Gson();
    }
    
    private String getAuthHeader() {
        String auth = config.getUsername() + ":" + config.getPassword();
        byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encodedAuth);
    }
    
    /**
     * Submit a batch job to Kyuubi
     */
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
            
            if (response.getStatusLine().getStatusCode() >= 200 && 
                response.getStatusLine().getStatusCode() < 300) {
                return gson.fromJson(responseBody, BatchResponse.class);
            } else {
                throw new IOException("Failed to submit batch: " + response.getStatusLine() + 
                    ", response: " + responseBody);
            }
        }
    }
    
    /**
     * Get batch status
     */
    public BatchResponse getBatch(String batchId) throws IOException {
        String url = config.getBaseUrl() + "/batches/" + batchId;
        
        HttpGet get = new HttpGet(url);
        get.setHeader(HttpHeaders.AUTHORIZATION, getAuthHeader());
        
        try (CloseableHttpResponse response = httpClient.execute(get)) {
            HttpEntity entity = response.getEntity();
            String responseBody = EntityUtils.toString(entity, StandardCharsets.UTF_8);
            
            if (response.getStatusLine().getStatusCode() >= 200 && 
                response.getStatusLine().getStatusCode() < 300) {
                return gson.fromJson(responseBody, BatchResponse.class);
            } else {
                throw new IOException("Failed to get batch: " + response.getStatusLine() + 
                    ", response: " + responseBody);
            }
        }
    }
    
    /**
     * Get batch logs
     */
    public LogResponse getBatchLogs(String batchId, int from, int size) throws IOException {
        String url = config.getBaseUrl() + "/batches/" + batchId + "/localLog?from=" + from + "&size=" + size;
        
        HttpGet get = new HttpGet(url);
        get.setHeader(HttpHeaders.AUTHORIZATION, getAuthHeader());
        
        try (CloseableHttpResponse response = httpClient.execute(get)) {
            HttpEntity entity = response.getEntity();
            String responseBody = EntityUtils.toString(entity, StandardCharsets.UTF_8);
            
            if (response.getStatusLine().getStatusCode() >= 200 && 
                response.getStatusLine().getStatusCode() < 300) {
                return gson.fromJson(responseBody, LogResponse.class);
            } else {
                throw new IOException("Failed to get batch logs: " + response.getStatusLine() + 
                    ", response: " + responseBody);
            }
        }
    }
    
    /**
     * Kill a batch job
     */
    public void killBatch(String batchId) throws IOException {
        String url = config.getBaseUrl() + "/batches/" + batchId;
        
        HttpDelete delete = new HttpDelete(url);
        delete.setHeader(HttpHeaders.AUTHORIZATION, getAuthHeader());
        
        try (CloseableHttpResponse response = httpClient.execute(delete)) {
            if (response.getStatusLine().getStatusCode() < 200 || 
                response.getStatusLine().getStatusCode() >= 300) {
                HttpEntity entity = response.getEntity();
                String responseBody = entity != null ? EntityUtils.toString(entity, StandardCharsets.UTF_8) : "";
                throw new IOException("Failed to kill batch: " + response.getStatusLine() + 
                    ", response: " + responseBody);
            }
        }
    }
    
    /**
     * Upload a file to Kyuubi server (requires kyuubi-upload-plugin).
     * The server uploads the file to the configured staging path (e.g., OSS)
     * and returns the remote URI.
     *
     * @return the remote URI (e.g., oss://bucket/.../query.sql)
     * @throws IOException on network error or non-2xx response
     */
    public String uploadFile(byte[] content, String fileName) throws IOException {
        String url = config.getBaseUrl() + "/files/upload";

        String boundary = "----SparkSubmitBoundary" + System.currentTimeMillis();

        ByteArrayOutputStream body = new ByteArrayOutputStream();
        byte[] header = ("--" + boundary + "\r\n"
                + "Content-Disposition: form-data; name=\"file\"; filename=\"" + fileName + "\"\r\n"
                + "Content-Type: application/octet-stream\r\n"
                + "\r\n").getBytes(StandardCharsets.UTF_8);
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
            String responseBody = entity != null
                    ? EntityUtils.toString(entity, StandardCharsets.UTF_8) : "";

            if (statusCode >= 200 && statusCode < 300) {
                JsonObject json = gson.fromJson(responseBody, JsonObject.class);
                if (json != null && json.has("uri")) {
                    return json.get("uri").getAsString();
                }
                throw new IOException("Upload succeeded but response missing 'uri': " + responseBody);
            } else {
                throw new IOException("Failed to upload file (HTTP " + statusCode + "): "
                        + response.getStatusLine() + ", response: " + responseBody);
            }
        }
    }

    // =============================================
    // Session & Operation API (for spark-sql mode)
    // =============================================

    /**
     * Create a new session
     */
    public SessionResponse createSession(Map<String, String> configs) throws IOException {
        String url = config.getBaseUrl() + "/sessions";

        JsonObject requestBody = new JsonObject();
        if (configs != null && !configs.isEmpty()) {
            JsonObject confObj = new JsonObject();
            for (Map.Entry<String, String> entry : configs.entrySet()) {
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

            if (response.getStatusLine().getStatusCode() >= 200 &&
                response.getStatusLine().getStatusCode() < 300) {
                return gson.fromJson(responseBody, SessionResponse.class);
            } else {
                throw new IOException("Failed to create session: " + response.getStatusLine() +
                    ", response: " + responseBody);
            }
        }
    }

    /**
     * Close a session
     */
    public void closeSession(String sessionHandle) throws IOException {
        String url = config.getBaseUrl() + "/sessions/" + sessionHandle;

        HttpDelete delete = new HttpDelete(url);
        delete.setHeader(HttpHeaders.AUTHORIZATION, getAuthHeader());

        try (CloseableHttpResponse response = httpClient.execute(delete)) {
            if (response.getStatusLine().getStatusCode() < 200 ||
                response.getStatusLine().getStatusCode() >= 300) {
                HttpEntity entity = response.getEntity();
                String responseBody = entity != null ? EntityUtils.toString(entity, StandardCharsets.UTF_8) : "";
                throw new IOException("Failed to close session: " + response.getStatusLine() +
                    ", response: " + responseBody);
            }
        }
    }

    /**
     * Execute a SQL statement in a session
     */
    public OperationResponse executeStatement(String sessionHandle, String statement, boolean runAsync) throws IOException {
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

            if (response.getStatusLine().getStatusCode() >= 200 &&
                response.getStatusLine().getStatusCode() < 300) {
                return gson.fromJson(responseBody, OperationResponse.class);
            } else {
                throw new IOException("Failed to execute statement: " + response.getStatusLine() +
                    ", response: " + responseBody);
            }
        }
    }

    /**
     * Get operation event (status)
     */
    public OperationEvent getOperationEvent(String operationHandle) throws IOException {
        String url = config.getBaseUrl() + "/operations/" + operationHandle + "/event";

        HttpGet get = new HttpGet(url);
        get.setHeader(HttpHeaders.AUTHORIZATION, getAuthHeader());

        try (CloseableHttpResponse response = httpClient.execute(get)) {
            HttpEntity entity = response.getEntity();
            String responseBody = EntityUtils.toString(entity, StandardCharsets.UTF_8);

            if (response.getStatusLine().getStatusCode() >= 200 &&
                response.getStatusLine().getStatusCode() < 300) {
                return gson.fromJson(responseBody, OperationEvent.class);
            } else {
                throw new IOException("Failed to get operation event: " + response.getStatusLine() +
                    ", response: " + responseBody);
            }
        }
    }

    /**
     * Get operation result set metadata (column descriptions)
     */
    public ResultSetMetadata getResultSetMetadata(String operationHandle) throws IOException {
        String url = config.getBaseUrl() + "/operations/" + operationHandle + "/resultsetmetadata";

        HttpGet get = new HttpGet(url);
        get.setHeader(HttpHeaders.AUTHORIZATION, getAuthHeader());

        try (CloseableHttpResponse response = httpClient.execute(get)) {
            HttpEntity entity = response.getEntity();
            String responseBody = EntityUtils.toString(entity, StandardCharsets.UTF_8);

            if (response.getStatusLine().getStatusCode() >= 200 &&
                response.getStatusLine().getStatusCode() < 300) {
                return gson.fromJson(responseBody, ResultSetMetadata.class);
            } else {
                throw new IOException("Failed to get result set metadata: " + response.getStatusLine() +
                    ", response: " + responseBody);
            }
        }
    }

    /**
     * Get operation result row set
     */
    public RowSetResponse getOperationRowSet(String operationHandle, int maxRows, String fetchOrientation) throws IOException {
        String url = config.getBaseUrl() + "/operations/" + operationHandle +
            "/rowset?maxrows=" + maxRows + "&fetchorientation=" + fetchOrientation;

        HttpGet get = new HttpGet(url);
        get.setHeader(HttpHeaders.AUTHORIZATION, getAuthHeader());

        try (CloseableHttpResponse response = httpClient.execute(get)) {
            HttpEntity entity = response.getEntity();
            String responseBody = EntityUtils.toString(entity, StandardCharsets.UTF_8);

            if (response.getStatusLine().getStatusCode() >= 200 &&
                response.getStatusLine().getStatusCode() < 300) {
                return gson.fromJson(responseBody, RowSetResponse.class);
            } else {
                throw new IOException("Failed to get operation row set: " + response.getStatusLine() +
                    ", response: " + responseBody);
            }
        }
    }

    /**
     * Get operation log lines
     */
    public LogResponse getOperationLog(String operationHandle, int maxRows) throws IOException {
        String url = config.getBaseUrl() + "/operations/" + operationHandle + "/log?maxrows=" + maxRows;

        HttpGet get = new HttpGet(url);
        get.setHeader(HttpHeaders.AUTHORIZATION, getAuthHeader());

        try (CloseableHttpResponse response = httpClient.execute(get)) {
            HttpEntity entity = response.getEntity();
            String responseBody = EntityUtils.toString(entity, StandardCharsets.UTF_8);

            if (response.getStatusLine().getStatusCode() >= 200 &&
                response.getStatusLine().getStatusCode() < 300) {
                return gson.fromJson(responseBody, LogResponse.class);
            } else {
                throw new IOException("Failed to get operation log: " + response.getStatusLine() +
                    ", response: " + responseBody);
            }
        }
    }

    /**
     * Cancel or close an operation
     * @param action "cancel" or "close"
     */
    public void updateOperation(String operationHandle, String action) throws IOException {
        String url = config.getBaseUrl() + "/operations/" + operationHandle;

        JsonObject requestBody = new JsonObject();
        requestBody.addProperty("action", action);

        org.apache.http.client.methods.HttpPut put = new org.apache.http.client.methods.HttpPut(url);
        put.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
        put.setHeader(HttpHeaders.AUTHORIZATION, getAuthHeader());
        put.setEntity(new StringEntity(gson.toJson(requestBody), StandardCharsets.UTF_8));

        try (CloseableHttpResponse response = httpClient.execute(put)) {
            if (response.getStatusLine().getStatusCode() < 200 ||
                response.getStatusLine().getStatusCode() >= 300) {
                HttpEntity entity = response.getEntity();
                String responseBody = entity != null ? EntityUtils.toString(entity, StandardCharsets.UTF_8) : "";
                throw new IOException("Failed to " + action + " operation: " + response.getStatusLine() +
                    ", response: " + responseBody);
            }
        }
    }

    public void close() throws IOException {
        httpClient.close();
    }
    
    /**
     * Batch response model
     */
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
        
        // Getters and setters
        public String getId() { return id; }
        public void setId(String id) { this.id = id; }
        public String getUser() { return user; }
        public void setUser(String user) { this.user = user; }
        public String getBatchType() { return batchType; }
        public void setBatchType(String batchType) { this.batchType = batchType; }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public Long getAppStartTime() { return appStartTime; }
        public void setAppStartTime(Long appStartTime) { this.appStartTime = appStartTime; }
        public String getAppId() { return appId; }
        public void setAppId(String appId) { this.appId = appId; }
        public String getAppUrl() { return appUrl; }
        public void setAppUrl(String appUrl) { this.appUrl = appUrl; }
        public String getAppState() { return appState; }
        public void setAppState(String appState) { this.appState = appState; }
        public String getAppDiagnostic() { return appDiagnostic; }
        public void setAppDiagnostic(String appDiagnostic) { this.appDiagnostic = appDiagnostic; }
        public String getKyuubiInstance() { return kyuubiInstance; }
        public void setKyuubiInstance(String kyuubiInstance) { this.kyuubiInstance = kyuubiInstance; }
        public String getState() { return state; }
        public void setState(String state) { this.state = state; }
        public Long getCreateTime() { return createTime; }
        public void setCreateTime(Long createTime) { this.createTime = createTime; }
        public Long getEndTime() { return endTime; }
        public void setEndTime(Long endTime) { this.endTime = endTime; }
        
        public boolean isFinished() {
            return "FINISHED".equals(state) || "ERROR".equals(state) || "CANCELED".equals(state);
        }
    }
    
    /**
     * Log response model
     */
    public static class LogResponse {
        private java.util.List<String> logRowSet;
        private Integer rowCount;
        
        public java.util.List<String> getLogRowSet() { return logRowSet; }
        public void setLogRowSet(java.util.List<String> logRowSet) { this.logRowSet = logRowSet; }
        public Integer getRowCount() { return rowCount; }
        public void setRowCount(Integer rowCount) { this.rowCount = rowCount; }
    }

    /**
     * Session response model
     */
    public static class SessionResponse {
        private String identifier;
        private String kyuubiInstance;

        public String getIdentifier() { return identifier; }
        public void setIdentifier(String identifier) { this.identifier = identifier; }
        public String getKyuubiInstance() { return kyuubiInstance; }
        public void setKyuubiInstance(String kyuubiInstance) { this.kyuubiInstance = kyuubiInstance; }
    }

    /**
     * Operation response model (for executeStatement)
     */
    public static class OperationResponse {
        private String identifier;

        public String getIdentifier() { return identifier; }
        public void setIdentifier(String identifier) { this.identifier = identifier; }
    }

    /**
     * Operation event model
     */
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

        public String getStatementId() { return statementId; }
        public String getRemoteId() { return remoteId; }
        public String getStatement() { return statement; }
        public Boolean getShouldRunAsync() { return shouldRunAsync; }
        public String getState() { return state; }
        public Long getEventTime() { return eventTime; }
        public Long getCreateTime() { return createTime; }
        public Long getStartTime() { return startTime; }
        public Long getCompleteTime() { return completeTime; }
        public String getException() { return exception; }
        public String getSessionId() { return sessionId; }
        public String getSessionUser() { return sessionUser; }

        public boolean isTerminal() {
            if (state == null) return false;
            // Kyuubi may return states with or without _STATE suffix
            String s = state.replace("_STATE", "");
            return "FINISHED".equals(s) || "ERROR".equals(s) ||
                   "CANCELED".equals(s) || "CLOSED".equals(s) ||
                   "TIMEOUT".equals(s);
        }
    }

    /**
     * Result set metadata model
     */
    public static class ResultSetMetadata {
        private List<ColumnDesc> columns;

        public List<ColumnDesc> getColumns() { return columns; }
        public void setColumns(List<ColumnDesc> columns) { this.columns = columns; }
    }

    /**
     * Column description model
     */
    public static class ColumnDesc {
        private String columnName;
        private String dataType;
        private Integer columnIndex;
        private Integer precision;
        private Integer scale;
        private String comment;

        public String getColumnName() { return columnName; }
        public String getDataType() { return dataType; }
        public Integer getColumnIndex() { return columnIndex; }
        public Integer getPrecision() { return precision; }
        public Integer getScale() { return scale; }
        public String getComment() { return comment; }
    }

    /**
     * Row set response model
     */
    public static class RowSetResponse {
        private List<Row> rows;
        private Integer rowCount;

        public List<Row> getRows() { return rows; }
        public void setRows(List<Row> rows) { this.rows = rows; }
        public Integer getRowCount() { return rowCount; }
        public void setRowCount(Integer rowCount) { this.rowCount = rowCount; }
    }

    /**
     * Row model
     */
    public static class Row {
        private List<Field> fields;

        public List<Field> getFields() { return fields; }
        public void setFields(List<Field> fields) { this.fields = fields; }
    }

    /**
     * Field model
     */
    public static class Field {
        private String dataType;
        private Object value;

        public String getDataType() { return dataType; }
        public Object getValue() { return value; }
    }
}

