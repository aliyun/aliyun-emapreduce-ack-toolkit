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
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Kyuubi REST API client
 */
public class KyuubiClient {
    private final Config config;
    private final CloseableHttpClient httpClient;
    private final Gson gson;
    
    public KyuubiClient(Config config) {
        this.config = config;
        this.httpClient = HttpClients.createDefault();
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
        batchRequest.addProperty("resource", args.getResource());
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
}

