package com.aliyun.emr.ack.command;

import com.aliyun.emr.ack.cli.*;
import com.aliyun.emr.ack.client.*;
import com.aliyun.emr.ack.util.*;
import java.io.IOException;

/**
 * Polls a submitted batch to completion: prints status transitions, the live Spark UI link and logs
 * (the driver-log stream when active, otherwise the Kyuubi submission log), enforces the overall
 * timeout (kill + exit 124), drains final logs and reports the terminal summary. Shared by the jar
 * and SQL-batch run modes. Returns the process exit code; the caller owns the client and exit.
 */
final class BatchMonitor {
    private BatchMonitor() {}

    private static final int MAX_CONSECUTIVE_ERRORS = 5;

    /** Run the monitor loop for an already-submitted batch and return its {@link ExitCode}. */
    static int await(
            KyuubiClient client,
            Config config,
            SparkSubmitArgs submitArgs,
            KyuubiClient.BatchResponse response) {
        String batchId = response.getId();

        System.out.println("[" + Console.timestamp() + "] Batch submitted successfully!");
        System.out.println("Batch ID: " + batchId);
        if (response.getAppId() != null && !response.getAppId().isEmpty()) {
            System.out.println("Application ID: " + response.getAppId());
        }
        String appUrl =
                AppUrls.applicationUrl(config.getSparkHistoryServerUrl(), response.getAppId());
        if (appUrl != null && !appUrl.isEmpty()) {
            System.out.println("Application URL: " + appUrl);
        }
        System.out.println();
        System.out.println("[" + Console.timestamp() + "] Waiting for job to complete...");
        if (submitArgs.getTimeoutSeconds() != null) {
            System.out.println("Timeout: " + submitArgs.getTimeoutSeconds() + " seconds");
        }
        System.out.println("------------------------------------------");

        int logOffset = 0;
        boolean firstLogOutput = true;
        String lastState = response.getState();
        boolean printedSparkUi = false;
        int consecutiveErrors = 0;
        long startTimeMillis = System.currentTimeMillis();
        long lastActivityTime = System.currentTimeMillis();
        long lastHeartbeatLogTime = System.currentTimeMillis();
        Long timeoutMillis =
                submitArgs.getTimeoutSeconds() != null
                        ? submitArgs.getTimeoutSeconds() * 1000L
                        : null;

        // Stream the live Spark driver pod log to the console (unless --no-driver-log). This loop
        // still owns lifecycle, exit code and timeout/kill.
        DriverLogStreamer driverStreamer = DriverLogStreamer.start(client, submitArgs, batchId);

        while (true) {
            try {
                Thread.sleep(Polling.INTERVAL_MS);

                // Check for timeout
                if (timeoutMillis != null) {
                    long elapsedMillis = System.currentTimeMillis() - startTimeMillis;
                    if (elapsedMillis >= timeoutMillis) {
                        System.err.println(
                                "\n["
                                        + Console.timestamp()
                                        + "] Job timeout after "
                                        + submitArgs.getTimeoutSeconds()
                                        + " seconds.");
                        System.err.println("Attempting to kill the job...");
                        try {
                            client.killBatch(batchId);
                            System.err.println("Kill request sent for Batch ID: " + batchId);
                        } catch (IOException killError) {
                            System.err.println(
                                    "Warning: Failed to kill job: " + killError.getMessage());
                        }
                        stop(driverStreamer);
                        return ExitCode.TIMEOUT;
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
                        System.out.println(
                                "\n["
                                        + Console.timestamp()
                                        + "] [Status] "
                                        + lastState
                                        + " -> "
                                        + currentState
                                        + " (elapsed: "
                                        + Console.formatDuration(elapsedSec)
                                        + ")");
                        lastState = currentState;
                        lastActivityTime = System.currentTimeMillis();
                    }
                }

                // Print the live Spark UI link once the driver has registered (appUrl available).
                if (!printedSparkUi) {
                    String sparkUi = AppUrls.sparkUiUrl(config.getServerUrl(), status.getAppUrl());
                    if (sparkUi != null) {
                        System.out.println("[" + Console.timestamp() + "] Spark UI: " + sparkUi);
                        printedSparkUi = true;
                    }
                }

                // Fetch and print new submission logs. While the driver stream is the live console
                // source this is suppressed (the driver pod log carries the real output); it
                // resumes
                // if the stream falls back so the user is never left with no logs.
                boolean hasNewLogs = false;
                if (!streamingActive(driverStreamer)) {
                    try {
                        KyuubiClient.LogResponse logResponse =
                                client.getBatchLogs(batchId, logOffset, Polling.LOG_FETCH_SIZE);
                        if (logResponse.getLogRowSet() != null
                                && !logResponse.getLogRowSet().isEmpty()) {
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

                            while (logResponse.getLogRowSet() != null
                                    && logResponse.getLogRowSet().size()
                                            == Polling.LOG_FETCH_SIZE) {
                                logResponse =
                                        client.getBatchLogs(
                                                batchId, logOffset, Polling.LOG_FETCH_SIZE);
                                if (logResponse.getLogRowSet() != null
                                        && !logResponse.getLogRowSet().isEmpty()) {
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
                            System.err.println(
                                    "\n["
                                            + Console.timestamp()
                                            + "] Warning: Could not fetch logs: "
                                            + logError.getMessage());
                        }
                    }
                }

                // Treat fresh driver log lines as activity so heartbeats stay quiet while
                // streaming.
                boolean recentDriverActivity =
                        streamingActive(driverStreamer)
                                && (System.currentTimeMillis()
                                                - driverStreamer.lastActivityMillis())
                                        < Polling.HEARTBEAT_LOG_INTERVAL_MS;

                // Check if finished
                if (status.isFinished()) {
                    // Stop the driver stream first so its output is fully flushed before the final
                    // summary, and to avoid a stray reconnect after the pod is gone.
                    boolean streamingWasActive = streamingActive(driverStreamer);
                    stop(driverStreamer);

                    // A short-lived pod can finish before the background stream attaches, leaving
                    // the console empty. If streaming was active but never delivered a line, pull
                    // the now-complete driver log once so a quick job still shows its output.
                    if (streamingWasActive
                            && driverStreamer != null
                            && !driverStreamer.receivedAnyLine()) {
                        driverStreamer.drainFinalBackfill();
                    }

                    // Without a live driver stream, drain any remaining submission logs.
                    if (!streamingWasActive) {
                        try {
                            KyuubiClient.LogResponse finalLogs =
                                    client.getBatchLogs(batchId, logOffset, Polling.LOG_FETCH_SIZE);
                            while (finalLogs.getLogRowSet() != null
                                    && !finalLogs.getLogRowSet().isEmpty()) {
                                for (String logLine : finalLogs.getLogRowSet()) {
                                    System.out.println(logLine);
                                }
                                logOffset += finalLogs.getLogRowSet().size();
                                finalLogs =
                                        client.getBatchLogs(
                                                batchId, logOffset, Polling.LOG_FETCH_SIZE);
                            }
                        } catch (IOException e) {
                            // Ignore final log fetch errors
                        }
                    }

                    long totalElapsedSec = (System.currentTimeMillis() - startTimeMillis) / 1000;
                    System.out.println("\n------------------------------------------");
                    System.out.println("[" + Console.timestamp() + "] Job finished!");
                    System.out.println("Final State: " + status.getState());
                    System.out.println("Total Time: " + Console.formatDuration(totalElapsedSec));

                    if (status.getAppId() != null && !status.getAppId().isEmpty()) {
                        System.out.println("Application ID: " + status.getAppId());
                    }
                    String finalAppUrl =
                            AppUrls.applicationUrl(
                                    config.getSparkHistoryServerUrl(), status.getAppId());
                    if (finalAppUrl != null && !finalAppUrl.isEmpty()) {
                        System.out.println("Application URL: " + finalAppUrl);
                    }

                    if (status.getAppDiagnostic() != null
                            && !status.getAppDiagnostic().trim().isEmpty()) {
                        System.out.println("\n=== Diagnostic Information ===");
                        System.out.println(status.getAppDiagnostic());
                        System.out.println("=== End Diagnostic ===");
                    }

                    String finalState = status.getState();
                    if ("ERROR".equals(finalState) || "CANCELED".equals(finalState)) {
                        // The driver stream only shows the pod's own output; if the pod never
                        // started, the reason is only in the Kyuubi submission log.
                        if (streamingWasActive) {
                            dumpSubmissionLog(client, batchId);
                        }
                        System.out.println("\n❌ Job failed or was canceled.");
                        return ExitCode.ERROR;
                    } else {
                        System.out.println("\n✅ Job completed successfully!");
                        return ExitCode.SUCCESS;
                    }
                }

                // Heartbeat message when idle
                long timeSinceLastHeartbeat = System.currentTimeMillis() - lastHeartbeatLogTime;
                if (timeSinceLastHeartbeat >= Polling.HEARTBEAT_LOG_INTERVAL_MS) {
                    if (!hasNewLogs && !recentDriverActivity) {
                        long elapsedSec = (System.currentTimeMillis() - startTimeMillis) / 1000;
                        long idleMinutes = (System.currentTimeMillis() - lastActivityTime) / 60000;
                        System.out.println(
                                "["
                                        + Console.timestamp()
                                        + "] [Heartbeat] Still running... (state: "
                                        + lastState
                                        + ", elapsed: "
                                        + Console.formatDuration(elapsedSec)
                                        + ", idle: "
                                        + idleMinutes
                                        + "m)");
                    }
                    lastHeartbeatLogTime = System.currentTimeMillis();
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println(
                        "\n["
                                + Console.timestamp()
                                + "] Interrupted while waiting for job completion.");
                stop(driverStreamer);
                return ExitCode.INTERRUPTED;
            } catch (IOException e) {
                consecutiveErrors++;
                if (consecutiveErrors >= MAX_CONSECUTIVE_ERRORS) {
                    System.err.println(
                            "\n["
                                    + Console.timestamp()
                                    + "] Too many consecutive errors fetching status. Exiting.");
                    System.err.println("Last error: " + e.getMessage());
                    stop(driverStreamer);
                    return ExitCode.ERROR;
                } else if (consecutiveErrors == 1) {
                    System.err.println(
                            "\n["
                                    + Console.timestamp()
                                    + "] Error fetching status: "
                                    + e.getMessage());
                    System.err.println(
                            "Retrying... (will exit after "
                                    + MAX_CONSECUTIVE_ERRORS
                                    + " consecutive errors)");
                }
            }
        }
    }

    private static boolean streamingActive(DriverLogStreamer streamer) {
        return streamer != null && !streamer.hasFallenBack();
    }

    private static void stop(DriverLogStreamer streamer) {
        if (streamer != null) {
            streamer.stop();
        }
    }

    /**
     * Dump the Kyuubi server-side submission log for a failed batch. The live driver stream only
     * shows the driver pod's own output, so when the pod never started (image pull error, quota,
     * scheduling failure) the reason lives only in this submission log.
     */
    private static void dumpSubmissionLog(KyuubiClient client, String batchId) {
        try {
            int offset = 0;
            boolean printedHeader = false;
            KyuubiClient.LogResponse logs =
                    client.getBatchLogs(batchId, offset, Polling.LOG_FETCH_SIZE);
            while (logs.getLogRowSet() != null && !logs.getLogRowSet().isEmpty()) {
                if (!printedHeader) {
                    System.out.println("\n=== Kyuubi Submission Log ===");
                    printedHeader = true;
                }
                for (String line : logs.getLogRowSet()) {
                    System.out.println(line);
                }
                offset += logs.getLogRowSet().size();
                if (logs.getLogRowSet().size() < Polling.LOG_FETCH_SIZE) {
                    break;
                }
                logs = client.getBatchLogs(batchId, offset, Polling.LOG_FETCH_SIZE);
            }
            if (printedHeader) {
                System.out.println("=== End Submission Log ===");
            }
        } catch (IOException e) {
            System.err.println(
                    "["
                            + Console.timestamp()
                            + "] Warning: could not fetch the submission log: "
                            + e.getMessage());
        }
    }
}
