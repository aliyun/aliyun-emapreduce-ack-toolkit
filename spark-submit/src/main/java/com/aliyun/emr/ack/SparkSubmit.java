package com.aliyun.emr.ack;

import com.aliyun.emr.ack.cli.*;
import com.aliyun.emr.ack.client.*;
import com.aliyun.emr.ack.command.*;
import com.aliyun.emr.ack.util.*;
import java.io.IOException;

/**
 * CLI entry point. Parses arguments, loads and validates configuration, then dispatches to the
 * matching command/run mode and exits with its code. Each run mode ({@link JarSubmitMode}, {@link
 * SqlBatchMode}, {@link SqlSessionMode}) owns its own submission, monitoring and reporting and
 * returns an {@link ExitCode}; this class performs the single {@code System.exit} and client
 * cleanup.
 */
public class SparkSubmit {

    public static void main(String[] args) {
        if (args.length == 0
                || (args.length == 1 && ("--help".equals(args[0]) || "-h".equals(args[0])))) {
            Usage.print();
            System.exit(ExitCode.SUCCESS);
        }

        KyuubiClient client = null;
        int exitCode;
        try {
            SparkSubmitArgs submitArgs = SparkSubmitParser.parse(args);

            // Validate mutually exclusive operations
            if (submitArgs.getStatusBatchId() != null && submitArgs.getKillBatchId() != null) {
                System.err.println("Error: --status and --kill cannot be used together");
                System.exit(ExitCode.ERROR);
            }

            // Load configuration, apply command-line overrides (highest priority) and warn if
            // default
            Config config =
                    submitArgs.getConfigFile() != null
                            ? new Config(submitArgs.getConfigFile())
                            : new Config();
            config.applyOverrides(submitArgs);
            config.validateAndPrintWarning();

            // Backfill driver-log defaults from the config file (CLI flags win), then fail fast on
            // a
            // bad filter regex before anything is submitted.
            DriverLogStreamer.applyConfigDefaults(submitArgs, config);
            try {
                DriverLogStreamer.validateFilters(submitArgs);
            } catch (IllegalArgumentException e) {
                System.err.println("Error: " + e.getMessage());
                System.exit(ExitCode.ERROR);
            }

            client = new KyuubiClient(config);

            if (submitArgs.getStatusBatchId() != null) {
                exitCode = runStatus(client, config, submitArgs);
            } else if (submitArgs.getKillBatchId() != null) {
                exitCode = runKill(client, submitArgs);
            } else {
                // Client-only retry policies; filtered out before sending to Kyuubi/Spark.
                Retry.RetryConfig submitRetryCfg =
                        RetryConfigs.forSubmit(submitArgs.getConf(), config);
                Retry.RetryConfig uploadRetryCfg =
                        RetryConfigs.forUpload(submitArgs.getConf(), config);

                if (submitArgs.isSqlMode()) {
                    if (submitArgs.getSqlFile() != null && submitArgs.getSqlStatement() != null) {
                        System.err.println("Error: -f and -e cannot be used together");
                        System.exit(ExitCode.ERROR);
                    }
                    exitCode =
                            submitArgs.isSqlBatchMode()
                                    ? new SqlBatchMode(
                                                    submitArgs,
                                                    config,
                                                    client,
                                                    submitRetryCfg,
                                                    uploadRetryCfg)
                                            .run()
                                    : new SqlSessionMode(submitArgs, config, client).run();
                } else {
                    exitCode = new JarSubmitMode(submitArgs, config, client, submitRetryCfg).run();
                }
            }

        } catch (Retry.RetryInterruptedException e) {
            // Interrupted (Ctrl-C) during retry backoff — exit with the interrupt code.
            System.err.println("\n[" + Console.timestamp() + "] Interrupted.");
            closeQuietly(client);
            System.err.flush();
            System.exit(ExitCode.INTERRUPTED);
            return;
        } catch (Exception e) {
            System.err.println("\n❌ Error: " + e.getMessage());
            if (e.getCause() != null) {
                System.err.println("   Cause: " + e.getCause().getMessage());
            }
            e.printStackTrace();
            System.err.flush();
            closeQuietly(client);
            System.err.println("\nUse --help for usage information.");
            System.err.flush();
            System.exit(ExitCode.ERROR);
            return;
        }

        closeQuietly(client);
        System.exit(exitCode);
    }

    private static int runStatus(KyuubiClient client, Config config, SparkSubmitArgs submitArgs)
            throws IOException {
        KyuubiClient.BatchResponse status = client.getBatch(submitArgs.getStatusBatchId());
        System.out.println("Batch ID: " + status.getId());
        System.out.println("State: " + status.getState());
        if (status.getAppId() != null) {
            System.out.println("Application ID: " + status.getAppId());
        }
        // Live driver UI (only reachable while the batch is running).
        String sparkUi = AppUrls.sparkUiUrl(config.getServerUrl(), status.getAppUrl());
        if (sparkUi != null && !sparkUi.isEmpty()) {
            System.out.println("Spark UI: " + sparkUi);
        }
        String appUrl =
                AppUrls.applicationUrl(config.getSparkHistoryServerUrl(), status.getAppId());
        if (appUrl != null && !appUrl.isEmpty()) {
            System.out.println("Application URL: " + appUrl);
        }
        if (status.getAppDiagnostic() != null && !status.getAppDiagnostic().isEmpty()) {
            System.out.println("Diagnostic: " + status.getAppDiagnostic());
        }
        return ExitCode.SUCCESS;
    }

    private static int runKill(KyuubiClient client, SparkSubmitArgs submitArgs) throws IOException {
        client.killBatch(submitArgs.getKillBatchId());
        System.out.println("Kill request sent for Batch ID: " + submitArgs.getKillBatchId());
        return ExitCode.SUCCESS;
    }

    private static void closeQuietly(KyuubiClient client) {
        if (client != null) {
            try {
                client.close();
            } catch (IOException e) {
                // Ignore
            }
        }
    }
}
