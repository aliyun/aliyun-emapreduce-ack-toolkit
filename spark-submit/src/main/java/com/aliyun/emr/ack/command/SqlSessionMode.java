package com.aliyun.emr.ack.command;

import com.aliyun.emr.ack.cli.*;
import com.aliyun.emr.ack.client.*;
import com.aliyun.emr.ack.util.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Runs SQL ({@code -e}/{@code -f}) interactively via a Kyuubi session: creates the session, executes
 * each statement with live operation-log output and heartbeat/overall-timeout enforcement, prints the
 * result set as a table, and closes the session. Returns the process {@link ExitCode}.
 */
public final class SqlSessionMode {

    private static final int MAX_CONSECUTIVE_ERRORS = 5;
    private static final int RESULT_FETCH_SIZE = 1000;

    private final SparkSubmitArgs submitArgs;
    private final Config config;
    private final KyuubiClient client;

    public SqlSessionMode(SparkSubmitArgs submitArgs, Config config, KyuubiClient client) {
        this.submitArgs = submitArgs;
        this.config = config;
        this.client = client;
    }

    public int run() {
        String sessionHandle = null;
        int exitCode = ExitCode.ERROR;
        try {
            // Read SQL content
            String sqlContent;
            if (submitArgs.getSqlFile() != null) {
                sqlContent = Sql.readFile(submitArgs.getSqlFile());
                System.out.println("[" + Console.timestamp() + "] Reading SQL from file: " + submitArgs.getSqlFile());
            } else {
                sqlContent = submitArgs.getSqlStatement();
            }

            if (sqlContent == null || sqlContent.trim().isEmpty()) {
                System.err.println("Error: SQL content is empty");
                return ExitCode.ERROR;
            }

            List<String> statements = Sql.parseStatements(sqlContent);
            if (statements.isEmpty()) {
                System.err.println("Error: No valid SQL statements found");
                return ExitCode.ERROR;
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
                    if (KyuubiClient.isClientOnlyConf(entry.getKey())) {
                        continue; // client-only (e.g. retry tuning), not sent to Spark/Kyuubi
                    }
                    System.out.println("  " + entry.getKey() + " = " + entry.getValue());
                }
            }
            System.out.println("------------------------------------------");
            System.out.println();

            // Create session
            System.out.println("[" + Console.timestamp() + "] Creating Kyuubi session...");
            KyuubiClient.SessionResponse session = client.createSession(submitArgs.getConf());
            sessionHandle = session.getIdentifier();
            System.out.println("[" + Console.timestamp() + "] Session created: " + sessionHandle);
            System.out.println();

            // Track overall timeout
            long overallStartTimeMillis = System.currentTimeMillis();
            Long overallTimeoutMillis = submitArgs.getTimeoutSeconds() != null
                    ? submitArgs.getTimeoutSeconds() * 1000L : null;

            // Execute each statement
            exitCode = ExitCode.SUCCESS;
            for (int idx = 0; idx < statements.size(); idx++) {
                String sql = statements.get(idx);
                System.out.println("------------------------------------------");
                System.out.println("[" + Console.timestamp() + "] [" + (idx + 1) + "/" + statements.size()
                        + "] Executing: " + Console.truncateSql(sql, 200));
                System.out.println("------------------------------------------");

                try {
                    executeSingleStatement(sessionHandle, sql, overallTimeoutMillis, overallStartTimeMillis);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.err.println("\n[" + Console.timestamp() + "] Interrupted.");
                    exitCode = ExitCode.INTERRUPTED;
                    break;
                } catch (Exception e) {
                    System.err.println("\n[" + Console.timestamp() + "] Error executing statement: " + e.getMessage());
                    if (e.getMessage() != null
                            && (e.getMessage().contains("Heartbeat timeout") || e.getMessage().contains("Overall timeout"))) {
                        exitCode = ExitCode.TIMEOUT;
                    } else {
                        exitCode = ExitCode.ERROR;
                    }
                    break;
                }
                System.out.println();
            }

            // Close session
            System.out.println("------------------------------------------");
            System.out.println("[" + Console.timestamp() + "] Closing session: " + sessionHandle);
            client.closeSession(sessionHandle);
            sessionHandle = null;
            System.out.println("[" + Console.timestamp() + "] Session closed.");

            if (exitCode == ExitCode.SUCCESS) {
                System.out.println("\n[" + Console.timestamp() + "] All SQL statements completed successfully.");
            }

            return exitCode;

        } catch (Exception e) {
            System.err.println("\n[" + Console.timestamp() + "] Error: " + e.getMessage());
            if (e.getCause() != null) {
                System.err.println("   Cause: " + e.getCause().getMessage());
            }
            e.printStackTrace();
            // Best-effort close of a session left open by the error
            if (sessionHandle != null) {
                try {
                    System.err.println("[" + Console.timestamp() + "] Closing session due to error...");
                    client.closeSession(sessionHandle);
                } catch (IOException ex) {
                    // Ignore
                }
            }
            return exitCode;
        }
    }

    /**
     * Execute a single statement: submit async, poll for completion with live log output and
     * heartbeat/overall-timeout enforcement, then fetch and print its result set.
     */
    private void executeSingleStatement(String sessionHandle, String sql,
            Long overallTimeoutMillis, long overallStartTimeMillis) throws IOException, InterruptedException {
        long stmtStartTime = System.currentTimeMillis();

        KyuubiClient.OperationResponse opResponse = client.executeStatement(sessionHandle, sql, true);
        String operationHandle = opResponse.getIdentifier();
        System.out.println("[" + Console.timestamp() + "] Operation submitted: " + operationHandle);

        String lastState = null;
        long lastActivityTime = System.currentTimeMillis();
        long lastHeartbeatLogTime = System.currentTimeMillis();
        int consecutiveErrors = 0;

        while (true) {
            Thread.sleep(Polling.INTERVAL_MS);

            // Check overall timeout
            if (overallTimeoutMillis != null) {
                long elapsed = System.currentTimeMillis() - overallStartTimeMillis;
                if (elapsed >= overallTimeoutMillis) {
                    System.err.println("\n[" + Console.timestamp() + "] Overall timeout after "
                            + (overallTimeoutMillis / 1000) + " seconds. Canceling operation...");
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
                KyuubiClient.LogResponse logResponse = client.getOperationLog(operationHandle, Polling.LOG_FETCH_SIZE);
                if (logResponse.getLogRowSet() != null && !logResponse.getLogRowSet().isEmpty()) {
                    for (String line : logResponse.getLogRowSet()) {
                        System.out.println(line);
                    }
                    hasNewLogs = true;
                    lastActivityTime = System.currentTimeMillis();

                    while (logResponse.getLogRowSet() != null
                            && logResponse.getLogRowSet().size() == Polling.LOG_FETCH_SIZE) {
                        logResponse = client.getOperationLog(operationHandle, Polling.LOG_FETCH_SIZE);
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
                    System.err.println("[" + Console.timestamp() + "] Warning: Error fetching operation status: "
                            + e.getMessage() + " (retrying...)");
                }
                continue;
            }

            String currentState = event.getState();
            if (!currentState.equals(lastState)) {
                if (lastState != null) {
                    System.out.println("[" + Console.timestamp() + "] [Status] " + lastState + " -> " + currentState);
                } else {
                    System.out.println("[" + Console.timestamp() + "] [Status] " + currentState);
                }
                lastState = currentState;
                lastActivityTime = System.currentTimeMillis();
            }

            if (event.isTerminal()) {
                // Fetch any remaining logs before reporting result
                try {
                    KyuubiClient.LogResponse finalLogs = client.getOperationLog(operationHandle, Polling.LOG_FETCH_SIZE);
                    while (finalLogs.getLogRowSet() != null && !finalLogs.getLogRowSet().isEmpty()) {
                        for (String line : finalLogs.getLogRowSet()) {
                            System.out.println(line);
                        }
                        finalLogs = client.getOperationLog(operationHandle, Polling.LOG_FETCH_SIZE);
                    }
                } catch (IOException e) {
                    // Ignore
                }

                if ("ERROR".equals(currentState) || "CANCELED".equals(currentState) || "TIMEOUT".equals(currentState)
                        || "ERROR_STATE".equals(currentState) || "CANCELED_STATE".equals(currentState)
                        || "TIMEOUT_STATE".equals(currentState)) {
                    String errorMsg = "Statement " + currentState;
                    if (event.getException() != null && !event.getException().isEmpty()) {
                        System.err.println("\n[" + Console.timestamp() + "] === Full Exception ===");
                        System.err.println(event.getException());
                        System.err.println("=== End Exception ===\n");
                        errorMsg += ": " + Console.extractFirstLine(event.getException());
                    }
                    throw new IOException(errorMsg);
                }

                long stmtElapsedSec = (System.currentTimeMillis() - stmtStartTime) / 1000;
                System.out.println("[" + Console.timestamp() + "] Statement completed in " + Console.formatDuration(stmtElapsedSec));
                break;
            }

            // Check heartbeat timeout (no activity for 30 minutes)
            long idleTimeMs = System.currentTimeMillis() - lastActivityTime;
            if (idleTimeMs >= Polling.HEARTBEAT_TIMEOUT_MS) {
                System.err.println("\n[" + Console.timestamp() + "] Heartbeat timeout: no activity for 30 minutes. Canceling operation...");
                try {
                    client.updateOperation(operationHandle, "cancel");
                } catch (IOException e) {
                    // Ignore
                }
                throw new IOException("Heartbeat timeout: no activity for 30 minutes");
            }

            // Print periodic heartbeat message (every 1 minute when idle)
            long timeSinceLastHeartbeatLog = System.currentTimeMillis() - lastHeartbeatLogTime;
            if (timeSinceLastHeartbeatLog >= Polling.HEARTBEAT_LOG_INTERVAL_MS) {
                if (!hasNewLogs) {
                    long idleMinutes = idleTimeMs / 60000;
                    System.out.println("[" + Console.timestamp() + "] [Heartbeat] Still running... (state: " + currentState
                            + ", idle: " + idleMinutes + "m, timeout: 30m)");
                }
                lastHeartbeatLogTime = System.currentTimeMillis();
            }
        }

        fetchAndPrintResults(operationHandle);

        try {
            client.updateOperation(operationHandle, "close");
        } catch (IOException e) {
            // Ignore close errors
        }
    }

    /** Fetch result-set metadata and all rows, then print as a formatted table. */
    private void fetchAndPrintResults(String operationHandle) throws IOException {
        KyuubiClient.ResultSetMetadata metadata = client.getResultSetMetadata(operationHandle);
        List<KyuubiClient.ColumnDesc> columns = metadata.getColumns();

        if (columns == null || columns.isEmpty()) {
            System.out.println("(No result columns)");
            return;
        }

        List<List<String>> allRows = new ArrayList<>();
        while (true) {
            KyuubiClient.RowSetResponse rowSet = client.getOperationRowSet(operationHandle, RESULT_FETCH_SIZE, "FETCH_NEXT");
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
            if (rowSet.getRows().size() < RESULT_FETCH_SIZE) {
                break;
            }
        }

        Console.printResultTable(columns, allRows);
    }
}
