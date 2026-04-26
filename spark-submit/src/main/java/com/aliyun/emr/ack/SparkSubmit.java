package com.aliyun.emr.ack;

import java.io.IOException;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Main entry point for spark-submit command
 */
public class SparkSubmit {
    private static final int POLL_INTERVAL_MS = 2000; // 2 seconds
    private static final int LOG_FETCH_SIZE = 100;
    private static final long HEARTBEAT_TIMEOUT_MS = 30 * 60 * 1000L; // 30 minutes no-activity timeout
    private static final long HEARTBEAT_LOG_INTERVAL_MS = 60 * 1000L; // 1 minute between heartbeat log messages
    private static final int SQL_UPLOAD_THRESHOLD_BYTES = 10 * 1024; // 10KB, conservative for CJK chars + JSON escaping + K8s pod spec overhead
    
    /**
     * Build Spark History Server URL from application ID
     * Supports both traditional format (application_xxx) and K8s format (spark-xxx)
     * Format: http://history-server:port/history/{appId}/1/
     * Examples:
     *   - Traditional: http://history-server:18080/history/application_1234567890_0001/1/
     *   - K8s: http://history-server:18080/history/spark-d99461f259674299bfd3faf71acb902c/1/
     */
    private static String buildHistoryServerUrl(String historyServerBaseUrl, String appId) {
        if (historyServerBaseUrl == null || historyServerBaseUrl.isEmpty() || 
            appId == null || appId.isEmpty()) {
            return null;
        }
        
        // Ensure base URL doesn't end with /
        String baseUrl = historyServerBaseUrl.trim();
        if (baseUrl.endsWith("/")) {
            baseUrl = baseUrl.substring(0, baseUrl.length() - 1);
        }
        
        // Build History Server URL
        // Format: http://history-server:port/history/application_xxx/1/
        return baseUrl + "/history/" + appId + "/1/";
    }
    
    /**
     * Get application URL, build History Server URL from appId if configured
     * Only returns URL if spark.history.server.url is configured, ignores Kyuubi's appUrl
     */
    private static String getApplicationUrl(String historyServerUrl, String appId) {
        // Only build History Server URL if configured
        if (historyServerUrl != null && !historyServerUrl.isEmpty() && 
            appId != null && !appId.isEmpty()) {
            return buildHistoryServerUrl(historyServerUrl, appId);
        }
        
        return null;
    }
    
    public static void main(String[] args) {
        // Check for --help flag
        if (args.length == 0 || (args.length == 1 && ("--help".equals(args[0]) || "-h".equals(args[0])))) {
            printUsage();
            System.exit(0);
        }
        
        KyuubiClient client = null;
        try {
            // Parse arguments
            SparkSubmitArgs submitArgs = SparkSubmitParser.parse(args);
            
            // Validate mutually exclusive operations
            if (submitArgs.getStatusBatchId() != null && submitArgs.getKillBatchId() != null) {
                System.err.println("Error: --status and --kill cannot be used together");
                System.exit(1);
            }
            
            // Load configuration
            Config config;
            if (submitArgs.getConfigFile() != null) {
                config = new Config(submitArgs.getConfigFile());
            } else {
                config = new Config();
            }
            
            // Apply command-line overrides (highest priority)
            config.applyOverrides(submitArgs);
            
            // Validate and warn if using default config
            config.validateAndPrintWarning();
            
            // Create Kyuubi client and submit batch
            client = new KyuubiClient(config);
            
            // Handle status query
            if (submitArgs.getStatusBatchId() != null) {
                KyuubiClient.BatchResponse status = client.getBatch(submitArgs.getStatusBatchId());
                System.out.println("Batch ID: " + status.getId());
                System.out.println("State: " + status.getState());
                if (status.getAppId() != null) {
                    System.out.println("Application ID: " + status.getAppId());
                }
                String appUrl = getApplicationUrl(config.getSparkHistoryServerUrl(), status.getAppId());
                if (appUrl != null && !appUrl.isEmpty()) {
                    System.out.println("Application URL: " + appUrl);
                }
                if (status.getAppDiagnostic() != null && !status.getAppDiagnostic().isEmpty()) {
                    System.out.println("Diagnostic: " + status.getAppDiagnostic());
                }
                client.close();
                System.exit(0);
            }
            
            // Handle kill
            if (submitArgs.getKillBatchId() != null) {
                client.killBatch(submitArgs.getKillBatchId());
                System.out.println("Kill request sent for Batch ID: " + submitArgs.getKillBatchId());
                client.close();
                System.exit(0);
            }
            
            // Handle SQL mode (-f or -e)
            if (submitArgs.isSqlMode()) {
                // Validate mutually exclusive SQL options
                if (submitArgs.getSqlFile() != null && submitArgs.getSqlStatement() != null) {
                    System.err.println("Error: -f and -e cannot be used together");
                    System.exit(1);
                }
                if (submitArgs.isSqlBatchMode()) {
                    executeSqlBatchMode(submitArgs, config, client);
                } else {
                    executeSqlMode(submitArgs, config, client);
                }
                return;
            }
            
            // Validate required arguments for submission
            if (submitArgs.getResource() == null || submitArgs.getResource().isEmpty()) {
                System.err.println("Error: Resource (jar or python file) is required");
                System.err.println("\nUse --help for usage information.");
                System.exit(1);
            }
            
            if (!"PYSPARK".equals(submitArgs.getBatchType())) {
                if (submitArgs.getClassName() == null || submitArgs.getClassName().isEmpty()) {
                    System.err.println("Error: --class is required for Spark (non-PySpark) jobs");
                    System.err.println("\nUse --help for usage information.");
                    System.exit(1);
                }
            }
            
            // Validate and handle deploy-mode
            String deployMode = submitArgs.getDeployMode();
            if (deployMode != null && !deployMode.isEmpty()) {
                if ("client".equalsIgnoreCase(deployMode)) {
                    System.err.println("\n⚠️  Warning: --deploy-mode client is not supported in this environment.");
                    System.err.println("   Client mode requires the driver to run on the local machine,");
                    System.err.println("   which is not compatible with remote Kyuubi server submission.");
                    System.err.println("   Deploy mode will be automatically changed to 'cluster'.\n");
                    deployMode = "cluster";
                    submitArgs.setDeployMode(deployMode);
                    submitArgs.getConf().put("spark.submit.deployMode", deployMode);
                } else if (!"cluster".equalsIgnoreCase(deployMode)) {
                    System.err.println("\n⚠️  Warning: Invalid --deploy-mode value: " + deployMode);
                    System.err.println("   Only 'cluster' mode is supported. Using 'cluster' mode.\n");
                    deployMode = "cluster";
                    submitArgs.setDeployMode(deployMode);
                    submitArgs.getConf().put("spark.submit.deployMode", deployMode);
                } else {
                    deployMode = "cluster";
                    submitArgs.setDeployMode(deployMode);
                    submitArgs.getConf().put("spark.submit.deployMode", deployMode);
                }
            } else {
                // Default to cluster mode
                deployMode = "cluster";
                submitArgs.setDeployMode(deployMode);
                submitArgs.getConf().put("spark.submit.deployMode", deployMode);
            }
            
            System.out.println("==========================================");
            System.out.println("Submitting Spark job to Kyuubi Server");
            System.out.println("==========================================");
            System.out.println("Kyuubi Server URL: " + config.getServerUrl());
            System.out.println("Username: " + config.getUsername());
            System.out.println("------------------------------------------");
            if (!"PYSPARK".equals(submitArgs.getBatchType())) {
                System.out.println("Application Class: " + submitArgs.getClassName());
            } else {
                System.out.println("PySpark Script: " + submitArgs.getResource());
            }
            System.out.println("Resource: " + submitArgs.getResource());
            if (submitArgs.getName() != null && !submitArgs.getName().isEmpty()) {
                System.out.println("Job Name: " + submitArgs.getName());
            }
            if (!submitArgs.getConf().isEmpty()) {
                System.out.println("Configuration:");
                for (java.util.Map.Entry<String, String> entry : submitArgs.getConf().entrySet()) {
                    System.out.println("  " + entry.getKey() + " = " + entry.getValue());
                }
            }
            System.out.println("==========================================");
            System.out.println();
            if (submitArgs.getProxyUser() != null && !submitArgs.getProxyUser().isEmpty()) {
                System.out.println("Proxy User: " + submitArgs.getProxyUser());
            }
            if (submitArgs.getQueue() != null && !submitArgs.getQueue().isEmpty()) {
                System.out.println("Queue: " + submitArgs.getQueue());
            }
            if (deployMode != null && !deployMode.isEmpty()) {
                System.out.println("Deploy Mode: " + deployMode);
            }
            if (!submitArgs.getPyFiles().isEmpty()) {
                System.out.println("Py Files: " + String.join(",", submitArgs.getPyFiles()));
            }
            if (!submitArgs.getFiles().isEmpty()) {
                System.out.println("Files: " + String.join(",", submitArgs.getFiles()));
            }
            if (!submitArgs.getArchives().isEmpty()) {
                System.out.println("Archives: " + String.join(",", submitArgs.getArchives()));
            }
            if (!submitArgs.getJars().isEmpty()) {
                System.out.println("Jars: " + String.join(",", submitArgs.getJars()));
            }
            if (!submitArgs.getPackages().isEmpty()) {
                System.out.println("Packages: " + String.join(",", submitArgs.getPackages()));
            }
            
            // Submit batch
            KyuubiClient.BatchResponse response = client.submitBatch(submitArgs);
            String batchId = response.getId();
            
            System.out.println("[" + timestamp() + "] Batch submitted successfully!");
            System.out.println("Batch ID: " + batchId);
            if (response.getAppId() != null && !response.getAppId().isEmpty()) {
                System.out.println("Application ID: " + response.getAppId());
            }
            String appUrl = getApplicationUrl(config.getSparkHistoryServerUrl(), response.getAppId());
            if (appUrl != null && !appUrl.isEmpty()) {
                System.out.println("Application URL: " + appUrl);
            }
            System.out.println();
            System.out.println("[" + timestamp() + "] Waiting for job to complete...");
            if (submitArgs.getTimeoutSeconds() != null) {
                System.out.println("Timeout: " + submitArgs.getTimeoutSeconds() + " seconds");
            }
            System.out.println("------------------------------------------");

            // Poll for status and logs
            int logOffset = 0;
            boolean firstLogOutput = true;
            String lastState = response.getState();
            int consecutiveErrors = 0;
            final int MAX_CONSECUTIVE_ERRORS = 5;
            long startTimeMillis = System.currentTimeMillis();
            long lastActivityTime = System.currentTimeMillis();
            long lastHeartbeatLogTime = System.currentTimeMillis();
            Long timeoutMillis = submitArgs.getTimeoutSeconds() != null ?
                    submitArgs.getTimeoutSeconds() * 1000L : null;

            while (true) {
                try {
                    Thread.sleep(POLL_INTERVAL_MS);

                    // Check for timeout
                    if (timeoutMillis != null) {
                        long elapsedMillis = System.currentTimeMillis() - startTimeMillis;
                        if (elapsedMillis >= timeoutMillis) {
                            System.err.println("\n[" + timestamp() + "] Job timeout after " + submitArgs.getTimeoutSeconds() + " seconds.");
                            System.err.println("Attempting to kill the job...");
                            try {
                                client.killBatch(batchId);
                                System.err.println("Kill request sent for Batch ID: " + batchId);
                            } catch (IOException killError) {
                                System.err.println("Warning: Failed to kill job: " + killError.getMessage());
                            }
                            client.close();
                            System.exit(124);
                        }
                    }

                    // Get batch status
                    KyuubiClient.BatchResponse status = client.getBatch(batchId);
                    consecutiveErrors = 0;

                    // Print status update if changed
                    if (status.getState() != null) {
                        String currentState = status.getState();
                        if (!currentState.equals(lastState)) {
                            long elapsedSec = (System.currentTimeMillis() - startTimeMillis) / 1000;
                            System.out.println("\n[" + timestamp() + "] [Status] " + lastState + " -> " + currentState
                                    + " (elapsed: " + formatDuration(elapsedSec) + ")");
                            lastState = currentState;
                            lastActivityTime = System.currentTimeMillis();
                        }
                    }

                    // Fetch and print new logs
                    boolean hasNewLogs = false;
                    try {
                        KyuubiClient.LogResponse logResponse = client.getBatchLogs(batchId, logOffset, LOG_FETCH_SIZE);
                        if (logResponse.getLogRowSet() != null && !logResponse.getLogRowSet().isEmpty()) {
                            if (firstLogOutput) {
                                System.out.println("\n=== Job Logs ===");
                                firstLogOutput = false;
                            }
                            for (String logLine : logResponse.getLogRowSet()) {
                                System.out.println(logLine);
                            }
                            logOffset += logResponse.getLogRowSet().size();
                            hasNewLogs = true;
                            lastActivityTime = System.currentTimeMillis();

                            while (logResponse.getLogRowSet() != null &&
                                   logResponse.getLogRowSet().size() == LOG_FETCH_SIZE) {
                                logResponse = client.getBatchLogs(batchId, logOffset, LOG_FETCH_SIZE);
                                if (logResponse.getLogRowSet() != null && !logResponse.getLogRowSet().isEmpty()) {
                                    for (String logLine : logResponse.getLogRowSet()) {
                                        System.out.println(logLine);
                                    }
                                    logOffset += logResponse.getLogRowSet().size();
                                } else {
                                    break;
                                }
                            }
                        }
                    } catch (IOException logError) {
                        if (consecutiveErrors == 0) {
                            System.err.println("\n[" + timestamp() + "] Warning: Could not fetch logs: " + logError.getMessage());
                        }
                    }

                    // Check if finished
                    if (status.isFinished()) {
                        // Try to fetch any remaining logs
                        try {
                            KyuubiClient.LogResponse finalLogs = client.getBatchLogs(batchId, logOffset, LOG_FETCH_SIZE);
                            while (finalLogs.getLogRowSet() != null && !finalLogs.getLogRowSet().isEmpty()) {
                                for (String logLine : finalLogs.getLogRowSet()) {
                                    System.out.println(logLine);
                                }
                                logOffset += finalLogs.getLogRowSet().size();
                                finalLogs = client.getBatchLogs(batchId, logOffset, LOG_FETCH_SIZE);
                            }
                        } catch (IOException e) {
                            // Ignore final log fetch errors
                        }

                        long totalElapsedSec = (System.currentTimeMillis() - startTimeMillis) / 1000;
                        System.out.println("\n------------------------------------------");
                        System.out.println("[" + timestamp() + "] Job finished!");
                        System.out.println("Final State: " + status.getState());
                        System.out.println("Total Time: " + formatDuration(totalElapsedSec));

                        if (status.getAppId() != null && !status.getAppId().isEmpty()) {
                            System.out.println("Application ID: " + status.getAppId());
                        }
                        String finalAppUrl = getApplicationUrl(config.getSparkHistoryServerUrl(), status.getAppId());
                        if (finalAppUrl != null && !finalAppUrl.isEmpty()) {
                            System.out.println("Application URL: " + finalAppUrl);
                        }

                        if (status.getAppDiagnostic() != null && !status.getAppDiagnostic().trim().isEmpty()) {
                            System.out.println("\n=== Diagnostic Information ===");
                            System.out.println(status.getAppDiagnostic());
                            System.out.println("=== End Diagnostic ===");
                        }

                        String finalState = status.getState();
                        if ("ERROR".equals(finalState) || "CANCELED".equals(finalState)) {
                            System.out.println("\n❌ Job failed or was canceled.");
                            client.close();
                            System.exit(1);
                        } else {
                            System.out.println("\n✅ Job completed successfully!");
                            client.close();
                            System.exit(0);
                        }
                    }

                    // Heartbeat message when idle
                    long timeSinceLastHeartbeat = System.currentTimeMillis() - lastHeartbeatLogTime;
                    if (timeSinceLastHeartbeat >= HEARTBEAT_LOG_INTERVAL_MS) {
                        if (!hasNewLogs) {
                            long elapsedSec = (System.currentTimeMillis() - startTimeMillis) / 1000;
                            long idleMinutes = (System.currentTimeMillis() - lastActivityTime) / 60000;
                            System.out.println("[" + timestamp() + "] [Heartbeat] Still running... (state: " + lastState
                                    + ", elapsed: " + formatDuration(elapsedSec) + ", idle: " + idleMinutes + "m)");
                        }
                        lastHeartbeatLogTime = System.currentTimeMillis();
                    }

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.err.println("\n[" + timestamp() + "] Interrupted while waiting for job completion.");
                    client.close();
                    System.exit(130);
                } catch (IOException e) {
                    consecutiveErrors++;
                    if (consecutiveErrors >= MAX_CONSECUTIVE_ERRORS) {
                        System.err.println("\n[" + timestamp() + "] Too many consecutive errors fetching status. Exiting.");
                        System.err.println("Last error: " + e.getMessage());
                        client.close();
                        System.exit(1);
                    } else if (consecutiveErrors == 1) {
                        System.err.println("\n[" + timestamp() + "] Error fetching status: " + e.getMessage());
                        System.err.println("Retrying... (will exit after " + MAX_CONSECUTIVE_ERRORS + " consecutive errors)");
                    }
                }
            }
            
        } catch (Exception e) {
            System.err.println("\n❌ Error: " + e.getMessage());
            if (e.getCause() != null) {
                System.err.println("   Cause: " + e.getCause().getMessage());
            }
            e.printStackTrace();
            System.err.flush(); // Ensure error output is flushed
            if (client != null) {
                try {
                    client.close();
                } catch (IOException ex) {
                    // Ignore
                }
            }
            System.err.println("\nUse --help for usage information.");
            System.err.flush();
            System.exit(1);
        }
    }
    
    /**
     * Execute SQL in batch mode: submit SQL as a Spark batch job using SparkSQLCLIDriver.
     * Suitable for SQL execution in cluster mode with SparkSQLCLIDriver built into the image.
     * SQL content is passed via application args: -e <sql> or -f <file>.
     */
    private static void executeSqlBatchMode(SparkSubmitArgs submitArgs, Config config, KyuubiClient client) {
        try {
            // Force cluster mode
            submitArgs.setDeployMode("cluster");
            submitArgs.getConf().put("spark.submit.deployMode", "cluster");

            // Use SparkSQLCLIDriver as the main class (built into Spark image)
            submitArgs.setClassName("org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver");

            // Resource points to the built-in spark-sql-cli JAR in the image
            // Kyuubi requires a resource parameter, use local:// to reference the image's built-in JAR
            submitArgs.setResource("local:///opt/spark/jars/spark-sql-cli.jar");

            // Resolve SQL content
            String resolvedSqlContent;
            String displaySqlSource;
            if (submitArgs.getSqlFile() != null) {
                System.out.println("[" + timestamp() + "] [Batch] Reading SQL file locally: " + submitArgs.getSqlFile());
                resolvedSqlContent = readSqlFile(submitArgs.getSqlFile());
                displaySqlSource = "SQL File (read locally): " + submitArgs.getSqlFile();
            } else {
                resolvedSqlContent = submitArgs.getSqlStatement();
                displaySqlSource = "SQL: " + truncateSql(submitArgs.getSqlStatement(), 100);
            }

            if (resolvedSqlContent == null || resolvedSqlContent.trim().isEmpty()) {
                System.err.println("Error: SQL content is empty");
                client.close();
                System.exit(1);
            }

            // Build args for SparkSQLCLIDriver
            // For large SQL (>32KB), upload to OSS and use -f oss://... to avoid K8s pod spec size limits
            List<String> sqlArgs = new ArrayList<>();
            byte[] sqlBytes = resolvedSqlContent.getBytes(StandardCharsets.UTF_8);

            if (sqlBytes.length > SQL_UPLOAD_THRESHOLD_BYTES) {
                String remoteUrl = uploadSqlFile(client, sqlBytes, submitArgs.getConf(), config);
                sqlArgs.add("-f");
                sqlArgs.add(remoteUrl);
                displaySqlSource = "SQL File (uploaded): " + remoteUrl
                        + " (" + (sqlBytes.length / 1024) + " KB)";
            } else {
                sqlArgs.add("-e");
                sqlArgs.add(resolvedSqlContent);
            }
            // Append any additional args from user
            sqlArgs.addAll(submitArgs.getArgs());
            submitArgs.setArgs(sqlArgs);

            System.out.println("==========================================");
            System.out.println("Submitting Spark SQL Batch Job to Kyuubi");
            System.out.println("==========================================");
            System.out.println("Kyuubi Server URL: " + config.getServerUrl());
            System.out.println("Username: " + config.getUsername());
            System.out.println("------------------------------------------");
            System.out.println("Mode: Batch (SparkSQLCLIDriver cluster mode)");
            System.out.println("Class: " + submitArgs.getClassName());
            System.out.println(displaySqlSource);
            if (!submitArgs.getConf().isEmpty()) {
                System.out.println("Configuration:");
                for (Map.Entry<String, String> entry : submitArgs.getConf().entrySet()) {
                    System.out.println("  " + entry.getKey() + " = " + entry.getValue());
                }
            }
            System.out.println("==========================================");
            System.out.println();

            // Submit batch
            KyuubiClient.BatchResponse response = client.submitBatch(submitArgs);
            String batchId = response.getId();

            System.out.println("[" + timestamp() + "] Batch submitted successfully!");
            System.out.println("Batch ID: " + batchId);
            if (response.getAppId() != null && !response.getAppId().isEmpty()) {
                System.out.println("Application ID: " + response.getAppId());
            }
            String appUrl = getApplicationUrl(config.getSparkHistoryServerUrl(), response.getAppId());
            if (appUrl != null && !appUrl.isEmpty()) {
                System.out.println("Application URL: " + appUrl);
            }
            System.out.println();
            System.out.println("[" + timestamp() + "] Waiting for job to complete...");
            if (submitArgs.getTimeoutSeconds() != null) {
                System.out.println("Timeout: " + submitArgs.getTimeoutSeconds() + " seconds");
            }
            System.out.println("------------------------------------------");

            // Poll for status and logs
            int logOffset = 0;
            boolean firstLogOutput = true;
            String lastState = response.getState();
            int consecutiveErrors = 0;
            final int MAX_CONSECUTIVE_ERRORS = 5;
            long startTimeMillis = System.currentTimeMillis();
            long lastActivityTime = System.currentTimeMillis();
            long lastHeartbeatLogTime = System.currentTimeMillis();
            Long timeoutMillis = submitArgs.getTimeoutSeconds() != null ?
                    submitArgs.getTimeoutSeconds() * 1000L : null;

            while (true) {
                try {
                    Thread.sleep(POLL_INTERVAL_MS);

                    // Check for timeout
                    if (timeoutMillis != null) {
                        long elapsedMillis = System.currentTimeMillis() - startTimeMillis;
                        if (elapsedMillis >= timeoutMillis) {
                            System.err.println("\n[" + timestamp() + "] Job timeout after " + submitArgs.getTimeoutSeconds() + " seconds.");
                            System.err.println("Attempting to kill the job...");
                            try {
                                client.killBatch(batchId);
                                System.err.println("Kill request sent for Batch ID: " + batchId);
                            } catch (IOException killError) {
                                System.err.println("Warning: Failed to kill job: " + killError.getMessage());
                            }
                            client.close();
                            System.exit(124);
                        }
                    }

                    // Get batch status
                    KyuubiClient.BatchResponse status = client.getBatch(batchId);
                    consecutiveErrors = 0;

                    if (status.getState() != null) {
                        String currentState = status.getState();
                        if (!currentState.equals(lastState)) {
                            long elapsedSec = (System.currentTimeMillis() - startTimeMillis) / 1000;
                            System.out.println("\n[" + timestamp() + "] [Status] " + lastState + " -> " + currentState
                                    + " (elapsed: " + formatDuration(elapsedSec) + ")");
                            lastState = currentState;
                            lastActivityTime = System.currentTimeMillis();
                        }
                    }

                    // Fetch and print new logs
                    boolean hasNewLogs = false;
                    try {
                        KyuubiClient.LogResponse logResponse = client.getBatchLogs(batchId, logOffset, LOG_FETCH_SIZE);
                        if (logResponse.getLogRowSet() != null && !logResponse.getLogRowSet().isEmpty()) {
                            if (firstLogOutput) {
                                System.out.println("\n=== Job Logs ===");
                                firstLogOutput = false;
                            }
                            for (String logLine : logResponse.getLogRowSet()) {
                                System.out.println(logLine);
                            }
                            logOffset += logResponse.getLogRowSet().size();
                            hasNewLogs = true;
                            lastActivityTime = System.currentTimeMillis();

                            while (logResponse.getLogRowSet() != null &&
                                   logResponse.getLogRowSet().size() == LOG_FETCH_SIZE) {
                                logResponse = client.getBatchLogs(batchId, logOffset, LOG_FETCH_SIZE);
                                if (logResponse.getLogRowSet() != null && !logResponse.getLogRowSet().isEmpty()) {
                                    for (String logLine : logResponse.getLogRowSet()) {
                                        System.out.println(logLine);
                                    }
                                    logOffset += logResponse.getLogRowSet().size();
                                } else {
                                    break;
                                }
                            }
                        }
                    } catch (IOException logError) {
                        // Non-fatal
                    }

                    if (status.isFinished()) {
                        try {
                            KyuubiClient.LogResponse finalLogs = client.getBatchLogs(batchId, logOffset, LOG_FETCH_SIZE);
                            while (finalLogs.getLogRowSet() != null && !finalLogs.getLogRowSet().isEmpty()) {
                                for (String logLine : finalLogs.getLogRowSet()) {
                                    System.out.println(logLine);
                                }
                                logOffset += finalLogs.getLogRowSet().size();
                                finalLogs = client.getBatchLogs(batchId, logOffset, LOG_FETCH_SIZE);
                            }
                        } catch (IOException e) {
                            // Ignore
                        }

                        long totalElapsedSec = (System.currentTimeMillis() - startTimeMillis) / 1000;
                        System.out.println("\n------------------------------------------");
                        System.out.println("[" + timestamp() + "] Job finished!");
                        System.out.println("Final State: " + status.getState());
                        System.out.println("Total Time: " + formatDuration(totalElapsedSec));

                        if (status.getAppId() != null && !status.getAppId().isEmpty()) {
                            System.out.println("Application ID: " + status.getAppId());
                        }
                        String finalAppUrl = getApplicationUrl(config.getSparkHistoryServerUrl(), status.getAppId());
                        if (finalAppUrl != null && !finalAppUrl.isEmpty()) {
                            System.out.println("Application URL: " + finalAppUrl);
                        }
                        if (status.getAppDiagnostic() != null && !status.getAppDiagnostic().trim().isEmpty()) {
                            System.out.println("\n=== Diagnostic Information ===");
                            System.out.println(status.getAppDiagnostic());
                            System.out.println("=== End Diagnostic ===");
                        }

                        String finalState = status.getState();
                        if ("ERROR".equals(finalState) || "CANCELED".equals(finalState)) {
                            System.out.println("\n❌ Job failed or was canceled.");
                            client.close();
                            System.exit(1);
                        } else {
                            System.out.println("\n✅ Job completed successfully!");
                            client.close();
                            System.exit(0);
                        }
                    }

                    // Heartbeat message when idle
                    long timeSinceLastHeartbeat = System.currentTimeMillis() - lastHeartbeatLogTime;
                    if (timeSinceLastHeartbeat >= HEARTBEAT_LOG_INTERVAL_MS) {
                        if (!hasNewLogs) {
                            long elapsedSec = (System.currentTimeMillis() - startTimeMillis) / 1000;
                            long idleMinutes = (System.currentTimeMillis() - lastActivityTime) / 60000;
                            System.out.println("[" + timestamp() + "] [Heartbeat] Still running... (state: " + lastState
                                    + ", elapsed: " + formatDuration(elapsedSec) + ", idle: " + idleMinutes + "m)");
                        }
                        lastHeartbeatLogTime = System.currentTimeMillis();
                    }

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.err.println("\n[" + timestamp() + "] Interrupted while waiting for job completion.");
                    client.close();
                    System.exit(130);
                } catch (IOException e) {
                    consecutiveErrors++;
                    if (consecutiveErrors >= MAX_CONSECUTIVE_ERRORS) {
                        System.err.println("\n[" + timestamp() + "] Too many consecutive errors fetching status. Exiting.");
                        System.err.println("Last error: " + e.getMessage());
                        client.close();
                        System.exit(1);
                    } else if (consecutiveErrors == 1) {
                        System.err.println("\n[" + timestamp() + "] Error fetching status: " + e.getMessage());
                        System.err.println("Retrying... (will exit after " + MAX_CONSECUTIVE_ERRORS + " consecutive errors)");
                    }
                }
            }

        } catch (Exception e) {
            System.err.println("\n❌ Error: " + e.getMessage());
            e.printStackTrace();
            try { client.close(); } catch (IOException ex) { /* Ignore */ }
            System.exit(1);
        }
    }

    /**
     * Execute SQL mode: create session, execute SQL statements, print results, close session.
     * Features:
     * - Continuous operation log output during statement execution
     * - Heartbeat timeout: kills job if no activity for 30 minutes
     * - Overall timeout: kills job if --timeout is exceeded
     * - Exit codes: 0 (success), 1 (error), 124 (timeout), 130 (interrupted)
     */
    private static void executeSqlMode(SparkSubmitArgs submitArgs, Config config, KyuubiClient client) {
        String sessionHandle = null;
        int exitCode = 1;
        try {
            // Read SQL content
            String sqlContent;
            if (submitArgs.getSqlFile() != null) {
                sqlContent = readSqlFile(submitArgs.getSqlFile());
                System.out.println("[" + timestamp() + "] Reading SQL from file: " + submitArgs.getSqlFile());
            } else {
                sqlContent = submitArgs.getSqlStatement();
            }

            if (sqlContent == null || sqlContent.trim().isEmpty()) {
                System.err.println("Error: SQL content is empty");
                client.close();
                System.exit(1);
            }

            // Parse SQL statements (split by semicolon, ignoring empty ones)
            List<String> statements = parseSqlStatements(sqlContent);
            if (statements.isEmpty()) {
                System.err.println("Error: No valid SQL statements found");
                client.close();
                System.exit(1);
            }

            System.out.println("==========================================");
            System.out.println("Executing Spark SQL via Kyuubi Server");
            System.out.println("==========================================");
            System.out.println("Kyuubi Server URL: " + config.getServerUrl());
            System.out.println("Username: " + config.getUsername());
            System.out.println("SQL statements to execute: " + statements.size());
            System.out.println("Heartbeat timeout: 30 minutes");
            if (submitArgs.getTimeoutSeconds() != null) {
                System.out.println("Overall timeout: " + submitArgs.getTimeoutSeconds() + " seconds");
            }
            if (!submitArgs.getConf().isEmpty()) {
                System.out.println("Configuration:");
                for (Map.Entry<String, String> entry : submitArgs.getConf().entrySet()) {
                    System.out.println("  " + entry.getKey() + " = " + entry.getValue());
                }
            }
            System.out.println("------------------------------------------");
            System.out.println();

            // Create session
            System.out.println("[" + timestamp() + "] Creating Kyuubi session...");
            KyuubiClient.SessionResponse session = client.createSession(submitArgs.getConf());
            sessionHandle = session.getIdentifier();
            System.out.println("[" + timestamp() + "] Session created: " + sessionHandle);
            System.out.println();

            // Track overall timeout
            long overallStartTimeMillis = System.currentTimeMillis();
            Long overallTimeoutMillis = submitArgs.getTimeoutSeconds() != null ?
                submitArgs.getTimeoutSeconds() * 1000L : null;

            // Execute each statement
            exitCode = 0;
            for (int idx = 0; idx < statements.size(); idx++) {
                String sql = statements.get(idx);
                System.out.println("------------------------------------------");
                System.out.println("[" + timestamp() + "] [" + (idx + 1) + "/" + statements.size() + "] Executing: " + truncateSql(sql, 200));
                System.out.println("------------------------------------------");

                try {
                    executeSingleStatement(client, sessionHandle, sql, overallTimeoutMillis, overallStartTimeMillis);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.err.println("\n[" + timestamp() + "] Interrupted.");
                    exitCode = 130;
                    break;
                } catch (Exception e) {
                    System.err.println("\n[" + timestamp() + "] Error executing statement: " + e.getMessage());
                    if (e.getMessage() != null &&
                        (e.getMessage().contains("Heartbeat timeout") || e.getMessage().contains("Overall timeout"))) {
                        exitCode = 124;
                    } else {
                        exitCode = 1;
                    }
                    break;
                }
                System.out.println();
            }

            // Close session
            System.out.println("------------------------------------------");
            System.out.println("[" + timestamp() + "] Closing session: " + sessionHandle);
            client.closeSession(sessionHandle);
            sessionHandle = null;
            System.out.println("[" + timestamp() + "] Session closed.");

            if (exitCode == 0) {
                System.out.println("\n[" + timestamp() + "] All SQL statements completed successfully.");
            }

            client.close();
            System.exit(exitCode);

        } catch (Exception e) {
            System.err.println("\n[" + timestamp() + "] Error: " + e.getMessage());
            if (e.getCause() != null) {
                System.err.println("   Cause: " + e.getCause().getMessage());
            }
            e.printStackTrace();
            // Try to close session on error
            if (sessionHandle != null) {
                try {
                    System.err.println("[" + timestamp() + "] Closing session due to error...");
                    client.closeSession(sessionHandle);
                } catch (IOException ex) {
                    // Ignore
                }
            }
            try {
                client.close();
            } catch (IOException ex) {
                // Ignore
            }
            System.exit(exitCode);
        }
    }

    /**
     * Execute a single SQL statement: submit, poll for completion with log output and heartbeat, fetch and print results.
     * - Continuously fetches and prints operation logs
     * - Tracks heartbeat: resets on new logs or state changes
     * - Kills operation if no activity for 30 minutes (heartbeat timeout)
     * - Kills operation if overall timeout is exceeded
     */
    private static void executeSingleStatement(KyuubiClient client, String sessionHandle, String sql,
            Long overallTimeoutMillis, long overallStartTimeMillis) throws IOException, InterruptedException {
        long stmtStartTime = System.currentTimeMillis();

        // Execute statement asynchronously
        KyuubiClient.OperationResponse opResponse = client.executeStatement(sessionHandle, sql, true);
        String operationHandle = opResponse.getIdentifier();
        System.out.println("[" + timestamp() + "] Operation submitted: " + operationHandle);

        // Poll for operation completion with heartbeat tracking
        String lastState = null;
        long lastActivityTime = System.currentTimeMillis();
        long lastHeartbeatLogTime = System.currentTimeMillis();
        int consecutiveErrors = 0;
        final int MAX_CONSECUTIVE_ERRORS = 5;

        while (true) {
            Thread.sleep(POLL_INTERVAL_MS);

            // Check overall timeout
            if (overallTimeoutMillis != null) {
                long elapsed = System.currentTimeMillis() - overallStartTimeMillis;
                if (elapsed >= overallTimeoutMillis) {
                    System.err.println("\n[" + timestamp() + "] Overall timeout after " +
                        (overallTimeoutMillis / 1000) + " seconds. Canceling operation...");
                    try {
                        client.updateOperation(operationHandle, "cancel");
                    } catch (IOException e) {
                        // Ignore
                    }
                    throw new IOException("Overall timeout after " + (overallTimeoutMillis / 1000) + " seconds");
                }
            }

            // Fetch and print operation logs (non-fatal errors)
            boolean hasNewLogs = false;
            try {
                KyuubiClient.LogResponse logResponse = client.getOperationLog(operationHandle, LOG_FETCH_SIZE);
                if (logResponse.getLogRowSet() != null && !logResponse.getLogRowSet().isEmpty()) {
                    for (String line : logResponse.getLogRowSet()) {
                        System.out.println(line);
                    }
                    hasNewLogs = true;
                    lastActivityTime = System.currentTimeMillis();

                    // Continue fetching if we got a full page (might have more logs)
                    while (logResponse.getLogRowSet() != null &&
                           logResponse.getLogRowSet().size() == LOG_FETCH_SIZE) {
                        logResponse = client.getOperationLog(operationHandle, LOG_FETCH_SIZE);
                        if (logResponse.getLogRowSet() != null && !logResponse.getLogRowSet().isEmpty()) {
                            for (String line : logResponse.getLogRowSet()) {
                                System.out.println(line);
                            }
                            lastActivityTime = System.currentTimeMillis();
                        } else {
                            break;
                        }
                    }
                }
            } catch (IOException e) {
                // Non-fatal, continue polling
            }

            // Check operation status
            KyuubiClient.OperationEvent event;
            try {
                event = client.getOperationEvent(operationHandle);
                consecutiveErrors = 0;
            } catch (IOException e) {
                consecutiveErrors++;
                if (consecutiveErrors >= MAX_CONSECUTIVE_ERRORS) {
                    throw new IOException("Too many consecutive errors fetching operation status: " + e.getMessage());
                }
                if (consecutiveErrors == 1) {
                    System.err.println("[" + timestamp() + "] Warning: Error fetching operation status: " +
                        e.getMessage() + " (retrying...)");
                }
                continue;
            }

            String currentState = event.getState();
            if (!currentState.equals(lastState)) {
                if (lastState != null) {
                    System.out.println("[" + timestamp() + "] [Status] " + lastState + " -> " + currentState);
                } else {
                    System.out.println("[" + timestamp() + "] [Status] " + currentState);
                }
                lastState = currentState;
                lastActivityTime = System.currentTimeMillis();
            }

            if (event.isTerminal()) {
                // Fetch any remaining logs before reporting result
                try {
                    KyuubiClient.LogResponse finalLogs = client.getOperationLog(operationHandle, LOG_FETCH_SIZE);
                    while (finalLogs.getLogRowSet() != null && !finalLogs.getLogRowSet().isEmpty()) {
                        for (String line : finalLogs.getLogRowSet()) {
                            System.out.println(line);
                        }
                        finalLogs = client.getOperationLog(operationHandle, LOG_FETCH_SIZE);
                    }
                } catch (IOException e) {
                    // Ignore
                }

                if ("ERROR".equals(currentState) || "CANCELED".equals(currentState) || "TIMEOUT".equals(currentState) ||
                    "ERROR_STATE".equals(currentState) || "CANCELED_STATE".equals(currentState) || "TIMEOUT_STATE".equals(currentState)) {
                    String errorMsg = "Statement " + currentState;
                    if (event.getException() != null && !event.getException().isEmpty()) {
                        System.err.println("\n[" + timestamp() + "] === Full Exception ===");
                        System.err.println(event.getException());
                        System.err.println("=== End Exception ===\n");
                        errorMsg += ": " + extractFirstLine(event.getException());
                    }
                    throw new IOException(errorMsg);
                }

                long stmtElapsedSec = (System.currentTimeMillis() - stmtStartTime) / 1000;
                System.out.println("[" + timestamp() + "] Statement completed in " + formatDuration(stmtElapsedSec));
                break;
            }

            // Check heartbeat timeout (30 minutes no activity)
            long idleTimeMs = System.currentTimeMillis() - lastActivityTime;
            if (idleTimeMs >= HEARTBEAT_TIMEOUT_MS) {
                System.err.println("\n[" + timestamp() + "] Heartbeat timeout: no activity for 30 minutes. Canceling operation...");
                try {
                    client.updateOperation(operationHandle, "cancel");
                } catch (IOException e) {
                    // Ignore
                }
                throw new IOException("Heartbeat timeout: no activity for 30 minutes");
            }

            // Print periodic heartbeat message (every 1 minute when idle)
            long timeSinceLastHeartbeatLog = System.currentTimeMillis() - lastHeartbeatLogTime;
            if (timeSinceLastHeartbeatLog >= HEARTBEAT_LOG_INTERVAL_MS) {
                if (!hasNewLogs) {
                    long idleMinutes = idleTimeMs / 60000;
                    System.out.println("[" + timestamp() + "] [Heartbeat] Still running... (state: " + currentState +
                        ", idle: " + idleMinutes + "m, timeout: 30m)");
                }
                lastHeartbeatLogTime = System.currentTimeMillis();
            }
        }

        // Fetch and print result set
        fetchAndPrintResults(client, operationHandle);

        // Close the operation
        try {
            client.updateOperation(operationHandle, "close");
        } catch (IOException e) {
            // Ignore close errors
        }
    }

    /**
     * Fetch result set metadata and rows, then print as a formatted table
     */
    private static void fetchAndPrintResults(KyuubiClient client, String operationHandle) throws IOException {
        // Get column metadata
        KyuubiClient.ResultSetMetadata metadata = client.getResultSetMetadata(operationHandle);
        List<KyuubiClient.ColumnDesc> columns = metadata.getColumns();

        if (columns == null || columns.isEmpty()) {
            System.out.println("(No result columns)");
            return;
        }

        // Fetch all rows
        List<List<String>> allRows = new ArrayList<>();
        int fetchSize = 1000;
        while (true) {
            KyuubiClient.RowSetResponse rowSet = client.getOperationRowSet(operationHandle, fetchSize, "FETCH_NEXT");
            if (rowSet.getRows() == null || rowSet.getRows().isEmpty()) {
                break;
            }
            for (KyuubiClient.Row row : rowSet.getRows()) {
                List<String> rowValues = new ArrayList<>();
                if (row.getFields() != null) {
                    for (KyuubiClient.Field field : row.getFields()) {
                        rowValues.add(field.getValue() != null ? String.valueOf(field.getValue()) : "NULL");
                    }
                }
                allRows.add(rowValues);
            }
            // If we got fewer rows than requested, no more data
            if (rowSet.getRows().size() < fetchSize) {
                break;
            }
        }

        // Print formatted table
        printResultTable(columns, allRows);
    }

    /**
     * Print results as a formatted table (similar to spark-sql output)
     */
    private static void printResultTable(List<KyuubiClient.ColumnDesc> columns, List<List<String>> rows) {
        int numCols = columns.size();

        // Calculate column widths
        int[] widths = new int[numCols];
        for (int i = 0; i < numCols; i++) {
            widths[i] = columns.get(i).getColumnName().length();
        }
        for (List<String> row : rows) {
            for (int i = 0; i < numCols && i < row.size(); i++) {
                widths[i] = Math.max(widths[i], row.get(i).length());
            }
        }

        // Build separator line
        StringBuilder separator = new StringBuilder("+");
        for (int w : widths) {
            for (int j = 0; j < w + 2; j++) {
                separator.append("-");
            }
            separator.append("+");
        }
        String sep = separator.toString();

        // Print header
        System.out.println(sep);
        StringBuilder header = new StringBuilder("|");
        for (int i = 0; i < numCols; i++) {
            header.append(" ").append(padRight(columns.get(i).getColumnName(), widths[i])).append(" |");
        }
        System.out.println(header.toString());
        System.out.println(sep);

        // Print rows
        for (List<String> row : rows) {
            StringBuilder rowLine = new StringBuilder("|");
            for (int i = 0; i < numCols; i++) {
                String value = i < row.size() ? row.get(i) : "";
                rowLine.append(" ").append(padRight(value, widths[i])).append(" |");
            }
            System.out.println(rowLine.toString());
        }
        System.out.println(sep);

        // Print row count
        System.out.println(rows.size() + " row(s) in set");
    }

    /**
     * Read SQL content from a file
     */
    private static String readSqlFile(String filePath) throws IOException {
        File file = new File(filePath);
        if (!file.exists()) {
            throw new IOException("SQL file not found: " + filePath);
        }
        StringBuilder sb = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line).append("\n");
            }
        }
        return sb.toString();
    }

    /**
     * Parse SQL content into individual statements (split by semicolon)
     * Handles comments (-- and /* ... * /) and skips empty statements
     */
    private static List<String> parseSqlStatements(String sqlContent) {
        List<String> statements = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inSingleLineComment = false;
        boolean inMultiLineComment = false;
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;

        for (int i = 0; i < sqlContent.length(); i++) {
            char c = sqlContent.charAt(i);
            char next = (i + 1 < sqlContent.length()) ? sqlContent.charAt(i + 1) : 0;

            if (inSingleLineComment) {
                if (c == '\n') {
                    inSingleLineComment = false;
                    current.append(c);
                }
                continue;
            }

            if (inMultiLineComment) {
                if (c == '*' && next == '/') {
                    inMultiLineComment = false;
                    i++; // skip '/'
                }
                continue;
            }

            if (inSingleQuote) {
                current.append(c);
                if (c == '\'' && (i == 0 || sqlContent.charAt(i - 1) != '\\')) {
                    inSingleQuote = false;
                }
                continue;
            }

            if (inDoubleQuote) {
                current.append(c);
                if (c == '"' && (i == 0 || sqlContent.charAt(i - 1) != '\\')) {
                    inDoubleQuote = false;
                }
                continue;
            }

            // Check for comments
            if (c == '-' && next == '-') {
                inSingleLineComment = true;
                i++; // skip second '-'
                continue;
            }
            if (c == '/' && next == '*') {
                inMultiLineComment = true;
                i++; // skip '*'
                continue;
            }

            // Check for quotes
            if (c == '\'') {
                inSingleQuote = true;
                current.append(c);
                continue;
            }
            if (c == '"') {
                inDoubleQuote = true;
                current.append(c);
                continue;
            }

            // Check for statement separator
            if (c == ';') {
                String stmt = current.toString().trim();
                if (!stmt.isEmpty()) {
                    statements.add(stmt);
                }
                current.setLength(0);
                continue;
            }

            current.append(c);
        }

        // Add last statement if not empty (without trailing semicolon)
        String last = current.toString().trim();
        if (!last.isEmpty()) {
            statements.add(last);
        }

        return statements;
    }

    /**
     * Truncate SQL for display purposes
     */
    private static String truncateSql(String sql, int maxLen) {
        String oneLine = sql.replaceAll("\\s+", " ").trim();
        if (oneLine.length() > maxLen) {
            return oneLine.substring(0, maxLen) + "...";
        }
        return oneLine;
    }

    /**
     * Pad a string to the right with spaces
     */
    private static String padRight(String s, int width) {
        if (s.length() >= width) return s;
        StringBuilder sb = new StringBuilder(s);
        for (int i = s.length(); i < width; i++) {
            sb.append(' ');
        }
        return sb.toString();
    }

    private static String timestamp() {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
    }

    private static String formatDuration(long totalSeconds) {
        long hours = totalSeconds / 3600;
        long minutes = (totalSeconds % 3600) / 60;
        long seconds = totalSeconds % 60;
        if (hours > 0) return String.format("%dh %dm %ds", hours, minutes, seconds);
        if (minutes > 0) return String.format("%dm %ds", minutes, seconds);
        return String.format("%ds", seconds);
    }

    private static String extractFirstLine(String text) {
        if (text == null) return "";
        int newline = text.indexOf('\n');
        return newline >= 0 ? text.substring(0, newline) : text;
    }

    /**
     * Upload large SQL file using dual strategy:
     * 1. Try Kyuubi server-side upload (requires kyuubi-upload-plugin)
     * 2. Fallback to client-side OSS upload (requires OSS credentials in --conf)
     */
    private static String uploadSqlFile(KyuubiClient client, byte[] sqlBytes,
                                         Map<String, String> conf, Config config) throws IOException {
        System.err.println("[" + timestamp() + "] SQL content is " + (sqlBytes.length / 1024) +
                " KB (threshold: " + (SQL_UPLOAD_THRESHOLD_BYTES / 1024) + " KB), uploading...");

        // Strategy 1: Kyuubi server-side upload (zero client-side OSS config needed)
        try {
            String uri = client.uploadFile(sqlBytes, "query.sql");
            System.err.println("[" + timestamp() + "] SQL uploaded via Kyuubi server: " + uri);
            return uri;
        } catch (IOException e) {
            String msg = e.getMessage();
            boolean pluginNotAvailable = msg != null &&
                    (msg.contains("HTTP 404") || msg.contains("HTTP 405") || msg.contains("Not Found"));
            if (pluginNotAvailable) {
                System.err.println("[" + timestamp() + "] Kyuubi upload plugin not available, " +
                        "trying client-side OSS upload...");
            } else {
                throw e;
            }
        }

        // Strategy 2: Client-side OSS upload (fallback for older Kyuubi without plugin)
        return uploadSqlToOss(sqlBytes, conf, config);
    }

    private static String getOssConfig(String key, Map<String, String> conf, Config config) {
        String value = conf.get(key);
        if (value == null) {
            value = config.getProperty(key);
        }
        return value;
    }

    private static String uploadSqlToOss(byte[] sqlBytes, Map<String, String> conf,
                                          Config config) throws IOException {
        String accessKeyId = getOssConfig("spark.hadoop.fs.oss.accessKeyId", conf, config);
        String accessKeySecret = getOssConfig("spark.hadoop.fs.oss.accessKeySecret", conf, config);
        String endpoint = getOssConfig("spark.hadoop.fs.oss.endpoint", conf, config);
        String uploadPath = getOssConfig("spark.kubernetes.file.upload.path", conf, config);

        if (accessKeyId == null || accessKeySecret == null || endpoint == null || uploadPath == null) {
            StringBuilder missing = new StringBuilder();
            if (accessKeyId == null) missing.append("\n  spark.hadoop.fs.oss.accessKeyId=<your-access-key-id>");
            if (accessKeySecret == null) missing.append("\n  spark.hadoop.fs.oss.accessKeySecret=<your-access-key-secret>");
            if (endpoint == null) missing.append("\n  spark.hadoop.fs.oss.endpoint=<oss-endpoint>");
            if (uploadPath == null) missing.append("\n  spark.kubernetes.file.upload.path=oss://<bucket>/<staging-path>");
            throw new IOException("SQL content is " + (sqlBytes.length / 1024) + " KB, exceeds " +
                    (SQL_UPLOAD_THRESHOLD_BYTES / 1024) + " KB threshold. " +
                    "OSS upload is required but the following configurations are missing:" + missing +
                    "\n\nAdd via --conf or in ~/.spark-submit.conf");
        }

        String[] parsed = OssUploader.parseOssPath(uploadPath);
        if (parsed == null) {
            throw new IOException("Invalid spark.kubernetes.file.upload.path: " + uploadPath +
                    ". Expected format: oss://<bucket>/<path>");
        }

        String bucket = parsed[0];
        String basePath = parsed[1];
        String objectKey = basePath + "/spark-sql-upload/" + UUID.randomUUID().toString() + ".sql";
        String publicEndpoint = OssUploader.toPublicEndpoint(endpoint);

        System.out.println("[" + timestamp() + "] SQL content is " + (sqlBytes.length / 1024) +
                " KB, uploading to OSS...");

        org.apache.http.impl.client.CloseableHttpClient httpClient =
                org.apache.http.impl.client.HttpClients.createDefault();
        try {
            String ossUrl = OssUploader.upload(httpClient, publicEndpoint, bucket, objectKey,
                    sqlBytes, accessKeyId, accessKeySecret);
            System.out.println("[" + timestamp() + "] SQL uploaded to: " + ossUrl);
            return ossUrl;
        } finally {
            httpClient.close();
        }
    }

    private static void printUsage() {
        System.out.println("Spark Submit Client for Kyuubi Server");
        System.out.println("=====================================\n");
        System.out.println("Usage:");
        System.out.println("  spark-submit [options] <app jar | python file> [app arguments]");
        System.out.println("  spark-submit -e <sql-string> [options]");
        System.out.println("  spark-submit -f <sql-file> [options]\n");
        System.out.println("Options:");
        System.out.println("  --class <class name>          Application's main class (required for JAR)");
        System.out.println("  --name <name>                 Name of your application");
        System.out.println("  --num-executors <num>         Number of executors");
        System.out.println("  --driver-cores <cores>        Driver cores");
        System.out.println("  --driver-memory <memory>      Memory for driver (e.g., 1g, 512m)");
        System.out.println("  --executor-cores <cores>      Number of cores per executor");
        System.out.println("  --executor-memory <memory>    Memory per executor (e.g., 1g, 512m)");
        System.out.println("  --files <file1,file2>         Comma-separated files to distribute");
        System.out.println("  --py-files <py1,py2>          Comma-separated py files (PySpark only)");
        System.out.println("  --jars <jar1,jar2>            Comma-separated extra JARs");
        System.out.println("  --archives <a1,a2>            Comma-separated archives");
        System.out.println("  --queue <queueName>           Queue name");
        System.out.println("  --proxy-user <user>           Proxy user (sets hive.server2.proxy.user)");
        System.out.println("  --deploy-mode <mode>          Deploy mode (cluster/client, default: cluster)");
        System.out.println("                                Note: client mode is not supported and will be");
        System.out.println("                                automatically changed to cluster mode");
        System.out.println("  --conf <key>=<value>          Spark configuration property");
        System.out.println("  --status <batchId>            Query batch status");
        System.out.println("  --kill <batchId>              Kill a batch job");
        System.out.println("  --timeout <seconds>           Timeout for job completion in seconds.");
        System.out.println("                                If exceeded, the job will be killed and exit");
        System.out.println("                                with code 124");
        System.out.println("  -e <sql-string>               Execute the given SQL statement (spark-sql mode)");
        System.out.println("  -f <sql-file>                 Execute SQL from the given file (spark-sql mode)");
        System.out.println("  --session                     Use session mode for SQL (-e/-f) instead of default batch mode.");
        System.out.println("                                Session mode returns query results as a table.");
        System.out.println("  --kyuubi-url <url>            Kyuubi server URL (overrides all other config)");
        System.out.println("  --kyuubi-user <user>          Kyuubi username (overrides all other config)");
        System.out.println("  --kyuubi-password <pwd>       Kyuubi password (overrides all other config)");
        System.out.println("  --config-file <path>          Custom config file path");
        System.out.println("  --help, -h                    Show this help message\n");
        System.out.println("Spark SQL Mode:");
        System.out.println("  Use -e or -f to execute SQL statements via Kyuubi session (like spark-sql).");
        System.out.println("  Multiple statements separated by ';' are supported.\n");
        System.out.println("  Examples:");
        System.out.println("    spark-submit -e \"SHOW DATABASES\"");
        System.out.println("    spark-submit -e \"SELECT * FROM my_db.my_table LIMIT 10\"");
        System.out.println("    spark-submit -f /path/to/query.sql");
        System.out.println("    spark-submit -f /path/to/query.sql --conf spark.executor.memory=2g\n");
        System.out.println("Configuration:");
        System.out.println("  Configure Kyuubi server connection via one of the following (priority order):\n");
        System.out.println("  1. Command-line arguments (highest priority):");
        System.out.println("     --kyuubi-url http://your-kyuubi-server:port");
        System.out.println("     --kyuubi-user your-username");
        System.out.println("     --kyuubi-password your-password\n");
        System.out.println("  2. Configuration file:");
        System.out.println("     Create: ~/.spark-submit.conf (or use --config-file <path>)");
        System.out.println("     Content:");
        System.out.println("       kyuubi.server.url=http://your-kyuubi-server:port");
        System.out.println("       kyuubi.server.username=your-username");
        System.out.println("       kyuubi.server.password=your-password");
        System.out.println("       spark.history.server.url=http://your-history-server:port  # Optional\n");
        System.out.println("  3. Environment variables:");
        System.out.println("     export KYUUBI_SERVER_URL=http://your-kyuubi-server:port");
        System.out.println("     export KYUUBI_SERVER_USERNAME=your-username");
        System.out.println("     export KYUUBI_SERVER_PASSWORD=your-password");
        System.out.println("     export SPARK_HISTORY_SERVER_URL=http://your-history-server:port  # Optional\n");
        System.out.println("  4. System properties (lowest priority):");
        System.out.println("     -Dkyuubi.server.url=http://your-kyuubi-server:port");
        System.out.println("     -Dkyuubi.server.username=your-username");
        System.out.println("     -Dkyuubi.server.password=your-password");
        System.out.println("     -Dspark.history.server.url=http://your-history-server:port  # Optional\n");
        System.out.println("Resources:");
        System.out.println("  Recommended: upload JAR to OSS and use oss://bucket/path/app.jar");
        System.out.println("Examples:");
        System.out.println("  spark-submit --name spark-pi \\");
        System.out.println("               --conf spark.submit.deployMode=cluster \\");
        System.out.println("               --class org.apache.spark.examples.SparkPi \\");
        System.out.println("               oss://your-bucket/path/spark-examples_2.12-3.5.7.jar\n");
        System.out.println("  spark-submit --name pyspark-job \\");
        System.out.println("               --py-files oss://your-bucket/lib1.py \\");
        System.out.println("               --files oss://your-bucket/conf.yaml \\");
        System.out.println("               oss://your-bucket/jobs/main.py --arg1 value1\n");
    }
}

