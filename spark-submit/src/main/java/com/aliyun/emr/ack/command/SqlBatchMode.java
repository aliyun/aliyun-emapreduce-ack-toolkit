package com.aliyun.emr.ack.command;

import com.aliyun.emr.ack.cli.*;
import com.aliyun.emr.ack.client.*;
import com.aliyun.emr.ack.util.*;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Runs SQL ({@code -e}/{@code -f}) as a Spark batch job via {@code SparkSQLCLIDriver} in cluster
 * mode. SQL over the upload threshold is staged out of the pod spec (see {@link SqlUploader}) and
 * passed as {@code -f <url>}. Returns the process {@link ExitCode}.
 */
public final class SqlBatchMode {

    private final SparkSubmitArgs submitArgs;
    private final Config config;
    private final KyuubiClient client;
    private final Retry.RetryConfig submitRetryCfg;
    private final Retry.RetryConfig uploadRetryCfg;

    public SqlBatchMode(
            SparkSubmitArgs submitArgs,
            Config config,
            KyuubiClient client,
            Retry.RetryConfig submitRetryCfg,
            Retry.RetryConfig uploadRetryCfg) {
        this.submitArgs = submitArgs;
        this.config = config;
        this.client = client;
        this.submitRetryCfg = submitRetryCfg;
        this.uploadRetryCfg = uploadRetryCfg;
    }

    public int run() throws IOException {
        // Force cluster mode with SparkSQLCLIDriver (built into the Spark image). Kyuubi requires a
        // resource, so reference the image's built-in jar via local://.
        submitArgs.setDeployMode("cluster");
        submitArgs.getConf().put("spark.submit.deployMode", "cluster");
        submitArgs.setClassName("org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver");
        submitArgs.setResource("local:///opt/spark/jars/spark-sql-cli.jar");

        // Resolve SQL content
        String resolvedSqlContent;
        String displaySqlSource;
        if (submitArgs.getSqlFile() != null) {
            System.out.println(
                    "["
                            + Console.timestamp()
                            + "] [Batch] Reading SQL file locally: "
                            + submitArgs.getSqlFile());
            resolvedSqlContent = Sql.readFile(submitArgs.getSqlFile());
            displaySqlSource = "SQL File (read locally): " + submitArgs.getSqlFile();
        } else {
            resolvedSqlContent = submitArgs.getSqlStatement();
            displaySqlSource = "SQL: " + Console.truncateSql(submitArgs.getSqlStatement(), 100);
        }

        if (resolvedSqlContent == null || resolvedSqlContent.trim().isEmpty()) {
            System.err.println("Error: SQL content is empty");
            return ExitCode.ERROR;
        }

        // Build args for SparkSQLCLIDriver; large SQL is uploaded and passed as -f to avoid the K8s
        // pod spec size limit.
        List<String> sqlArgs = new ArrayList<>();
        byte[] sqlBytes = resolvedSqlContent.getBytes(StandardCharsets.UTF_8);
        if (sqlBytes.length > SqlUploader.THRESHOLD_BYTES) {
            String remoteUrl =
                    SqlUploader.upload(
                            client, sqlBytes, submitArgs.getConf(), config, uploadRetryCfg);
            sqlArgs.add("-f");
            sqlArgs.add(remoteUrl);
            displaySqlSource =
                    "SQL File (uploaded): " + remoteUrl + " (" + (sqlBytes.length / 1024) + " KB)";
        } else {
            sqlArgs.add("-e");
            sqlArgs.add(resolvedSqlContent);
        }
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
                if (KyuubiClient.isClientOnlyConf(entry.getKey())) {
                    continue; // client-only (e.g. retry tuning), not sent to Spark/Kyuubi
                }
                System.out.println("  " + entry.getKey() + " = " + entry.getValue());
            }
        }
        System.out.println("==========================================");
        System.out.println();

        // Submit batch (with retry on connection-phase failures only — never duplicates a job)
        KyuubiClient.BatchResponse response =
                RetryConfigs.submit(client, submitArgs, submitRetryCfg);
        return BatchMonitor.await(client, config, submitArgs, response);
    }
}
