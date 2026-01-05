package com.aliyun.emr.ack;

import java.io.IOException;

/**
 * Main entry point for spark-submit command
 */
public class SparkSubmit {
    private static final int POLL_INTERVAL_MS = 2000; // 2 seconds
    private static final int LOG_FETCH_SIZE = 100;
    
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
            Config config = new Config();
            
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
            
            System.out.println("✅ Batch submitted successfully!");
            System.out.println("Batch ID: " + batchId);
            if (response.getAppId() != null && !response.getAppId().isEmpty()) {
                System.out.println("Application ID: " + response.getAppId());
            }
            String appUrl = getApplicationUrl(config.getSparkHistoryServerUrl(), response.getAppId());
            if (appUrl != null && !appUrl.isEmpty()) {
                System.out.println("Application URL: " + appUrl);
            }
            System.out.println();
            System.out.println("Waiting for job to complete...");
            System.out.println("------------------------------------------");
            
            // Poll for status and logs
            int logOffset = 0;
            boolean firstLogOutput = true;
            String lastState = response.getState();
            int consecutiveErrors = 0;
            final int MAX_CONSECUTIVE_ERRORS = 5;
            
            while (true) {
                try {
                    Thread.sleep(POLL_INTERVAL_MS);
                    
                    // Get batch status
                    KyuubiClient.BatchResponse status = client.getBatch(batchId);
                    consecutiveErrors = 0; // Reset error counter on success
                    
                    // Print status update if changed
                    if (status.getState() != null) {
                        String currentState = status.getState();
                        if (!currentState.equals(lastState)) {
                            System.out.println("\n[Status] " + lastState + " -> " + currentState);
                            lastState = currentState;
                        }
                    }
                    
                    // Fetch and print new logs
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
                            
                            // Continue fetching if we got a full page (might have more logs)
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
                        // Log fetching errors are non-fatal, continue
                        if (consecutiveErrors == 0) {
                            System.err.println("\n⚠️  Warning: Could not fetch logs: " + logError.getMessage());
                        }
                    }
                    
                    // Check if finished
                    if (status.isFinished()) {
                        // Try to fetch any remaining logs
                        try {
                            KyuubiClient.LogResponse finalLogs = client.getBatchLogs(batchId, logOffset, LOG_FETCH_SIZE);
                            if (finalLogs.getLogRowSet() != null && !finalLogs.getLogRowSet().isEmpty()) {
                                for (String logLine : finalLogs.getLogRowSet()) {
                                    System.out.println(logLine);
                                }
                            }
                        } catch (IOException e) {
                            // Ignore final log fetch errors
                        }
                        
                        System.out.println("\n------------------------------------------");
                        System.out.println("Job finished!");
                        System.out.println("Final State: " + status.getState());
                        
                        if (status.getAppId() != null && !status.getAppId().isEmpty()) {
                            System.out.println("Application ID: " + status.getAppId());
                        }
                        String finalAppUrl = getApplicationUrl(config.getSparkHistoryServerUrl(), status.getAppId());
                        if (finalAppUrl != null && !finalAppUrl.isEmpty()) {
                            System.out.println("Application URL: " + finalAppUrl);
                        }
                        
                        if (status.getAppDiagnostic() != null && !status.getAppDiagnostic().isEmpty() && 
                            !status.getAppDiagnostic().trim().isEmpty()) {
                            System.out.println("\nDiagnostic Information:");
                            System.out.println(status.getAppDiagnostic());
                        }
                        
                        // Exit with appropriate code
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
                    
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.err.println("\n⚠️  Interrupted while waiting for job completion.");
                    client.close();
                    System.exit(130);
                } catch (IOException e) {
                    consecutiveErrors++;
                    if (consecutiveErrors >= MAX_CONSECUTIVE_ERRORS) {
                        System.err.println("\n❌ Too many consecutive errors fetching status. Exiting.");
                        System.err.println("Last error: " + e.getMessage());
                        client.close();
                        System.exit(1);
                    } else if (consecutiveErrors == 1) {
                        System.err.println("\n⚠️  Error fetching status: " + e.getMessage());
                        System.err.println("Retrying... (will exit after " + MAX_CONSECUTIVE_ERRORS + " consecutive errors)");
                    }
                    // Continue polling in case of temporary network issues
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
    
    private static void printUsage() {
        System.out.println("Spark Submit Client for Kyuubi Server");
        System.out.println("=====================================\n");
        System.out.println("Usage: spark-submit [options] <app jar | python file> [app arguments]\n");
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
        System.out.println("  --help, -h                    Show this help message\n");
        System.out.println("Configuration:");
        System.out.println("  Configure Kyuubi server connection via one of the following:\n");
        System.out.println("  1. Configuration file (recommended):");
        System.out.println("     Create: ~/.spark-submit.conf");
        System.out.println("     Content:");
        System.out.println("       kyuubi.server.url=http://your-kyuubi-server:port");
        System.out.println("       kyuubi.server.username=your-username");
        System.out.println("       kyuubi.server.password=your-password");
        System.out.println("       spark.history.server.url=http://your-history-server:port  # Optional\n");
        System.out.println("  2. Environment variables:");
        System.out.println("     export KYUUBI_SERVER_URL=http://your-kyuubi-server:port");
        System.out.println("     export KYUUBI_SERVER_USERNAME=your-username");
        System.out.println("     export KYUUBI_SERVER_PASSWORD=your-password");
        System.out.println("     export SPARK_HISTORY_SERVER_URL=http://your-history-server:port  # Optional\n");
        System.out.println("  3. System properties:");
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

