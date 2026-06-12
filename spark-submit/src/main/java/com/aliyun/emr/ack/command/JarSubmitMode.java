package com.aliyun.emr.ack.command;

import com.aliyun.emr.ack.cli.*;
import com.aliyun.emr.ack.client.*;
import com.aliyun.emr.ack.util.*;

import java.io.IOException;
import java.util.Map;

/**
 * Submits a JAR (or PySpark) application as a Kyuubi batch: validates the required arguments,
 * normalises the deploy mode to {@code cluster}, prints the submission banner, submits with retry and
 * hands off to {@link BatchMonitor}. Returns the process {@link ExitCode}.
 */
public final class JarSubmitMode {

    private final SparkSubmitArgs submitArgs;
    private final Config config;
    private final KyuubiClient client;
    private final Retry.RetryConfig submitRetryCfg;

    public JarSubmitMode(SparkSubmitArgs submitArgs, Config config, KyuubiClient client,
                  Retry.RetryConfig submitRetryCfg) {
        this.submitArgs = submitArgs;
        this.config = config;
        this.client = client;
        this.submitRetryCfg = submitRetryCfg;
    }

    public int run() throws IOException {
        // Validate required arguments for submission
        if (submitArgs.getResource() == null || submitArgs.getResource().isEmpty()) {
            System.err.println("Error: Resource (jar or python file) is required");
            System.err.println("\nUse --help for usage information.");
            return ExitCode.ERROR;
        }
        if (!"PYSPARK".equals(submitArgs.getBatchType())) {
            if (submitArgs.getClassName() == null || submitArgs.getClassName().isEmpty()) {
                System.err.println("Error: --class is required for Spark (non-PySpark) jobs");
                System.err.println("\nUse --help for usage information.");
                return ExitCode.ERROR;
            }
        }

        String deployMode = normalizeDeployMode();

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
            for (Map.Entry<String, String> entry : submitArgs.getConf().entrySet()) {
                if (KyuubiClient.isClientOnlyConf(entry.getKey())) {
                    continue; // client-only (e.g. retry tuning), not sent to Spark/Kyuubi
                }
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

        // Submit batch (with retry on connection-phase failures only — never duplicates a job)
        KyuubiClient.BatchResponse response = RetryConfigs.submit(client, submitArgs, submitRetryCfg);
        return BatchMonitor.await(client, config, submitArgs, response);
    }

    /** Force cluster mode (client mode is unsupported with remote Kyuubi submission); warn if changed. */
    private String normalizeDeployMode() {
        String deployMode = submitArgs.getDeployMode();
        if (deployMode != null && !deployMode.isEmpty()) {
            if ("client".equalsIgnoreCase(deployMode)) {
                System.err.println("\n⚠️  Warning: --deploy-mode client is not supported in this environment.");
                System.err.println("   Client mode requires the driver to run on the local machine,");
                System.err.println("   which is not compatible with remote Kyuubi server submission.");
                System.err.println("   Deploy mode will be automatically changed to 'cluster'.\n");
            } else if (!"cluster".equalsIgnoreCase(deployMode)) {
                System.err.println("\n⚠️  Warning: Invalid --deploy-mode value: " + deployMode);
                System.err.println("   Only 'cluster' mode is supported. Using 'cluster' mode.\n");
            }
        }
        submitArgs.setDeployMode("cluster");
        submitArgs.getConf().put("spark.submit.deployMode", "cluster");
        return "cluster";
    }
}
