package com.aliyun.emr.ack;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeNotNull;

import com.aliyun.emr.ack.cli.SparkSubmitArgs;
import com.aliyun.emr.ack.client.Config;
import com.aliyun.emr.ack.client.KyuubiClient;
import com.aliyun.emr.ack.command.DriverLogFilter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

/**
 * End-to-end tests against a real Kyuubi server with Spark-on-Kubernetes. They cover every client
 * link delivered to customers:
 *
 * <ul>
 *   <li>batch submit -&gt; status poll -&gt; driver-log SSE stream -&gt; client-side filter -&gt;
 *       FINISHED;
 *   <li>SQL session -&gt; execute statement -&gt; operation log/event -&gt; result-set metadata +
 *       rows -&gt; close;
 *   <li>status query, local (submission) log, and kill of a running batch.
 * </ul>
 *
 * <p>Opt-in: every test is skipped unless {@code KYUUBI_E2E_URL} is set, so the offline build stays
 * green. Run against zhigang-ack (with JaCoCo coverage) via:
 *
 * <pre>
 *   KYUUBI_E2E_URL=http://47.239.194.145:10099 KYUUBI_E2E_USER=e2e \
 *     mvn test -Dtest=KyuubiE2ETest    # report at target/site/jacoco/index.html
 * </pre>
 *
 * Optional overrides: {@code KYUUBI_E2E_IMAGE}, {@code KYUUBI_E2E_NAMESPACE}, {@code
 * KYUUBI_E2E_SA}, {@code KYUUBI_E2E_JAR}.
 */
public class KyuubiE2ETest {

    private String url;
    private KyuubiClient client;

    private static String env(String key, String dflt) {
        String v = System.getenv(key);
        return (v == null || v.isEmpty()) ? dflt : v;
    }

    @Before
    public void setUp() {
        url = System.getenv("KYUUBI_E2E_URL");
        assumeNotNull(url); // skip the whole class when no cluster is configured

        SparkSubmitArgs conn = new SparkSubmitArgs();
        conn.setKyuubiUrl(url);
        conn.setKyuubiUser(env("KYUUBI_E2E_USER", "e2e"));
        Config config = new Config("/tmp/__e2e_nonexistent.conf");
        config.applyOverrides(conn);
        client = new KyuubiClient(config);
    }

    // ---- Link 1: batch submit + driver-log stream + filter + terminal state ----

    @Test
    public void batchSubmit_streamsDriverLog_filtersAndFinishes() throws Exception {
        String batchId = client.submitBatch(sparkPiArgs("100")).getId();
        assertNotNull("batch id", batchId);

        CollectingHandler handler = streamToEnd(batchId, 6 * 60 * 1000L);
        assertTrue(
                "expected many driver log lines, got " + handler.lines.size(),
                handler.lines.size() > 50);
        assertNotNull("expected a stream 'end' event", handler.endReason);
        assertTrue(
                "driver log should contain the Pi result",
                handler.lines.stream().anyMatch(l -> l.contains("Pi is roughly")));

        KyuubiClient.BatchResponse finalState = awaitTerminal(batchId, 60 * 1000L);
        assertTrue(
                "expected FINISHED, was " + finalState.getState(),
                "FINISHED".equals(finalState.getState()));

        DriverLogFilter filter = DriverLogFilter.fromRegexes(null, "TaskSetManager");
        long kept = handler.lines.stream().filter(filter::shouldPrint).count();
        assertTrue(
                "filter should drop some real lines (" + kept + "/" + handler.lines.size() + ")",
                kept < handler.lines.size());
        assertTrue(
                "no kept line should contain the excluded pattern",
                handler.lines.stream()
                        .filter(filter::shouldPrint)
                        .noneMatch(l -> l.contains("TaskSetManager")));
    }

    // ---- Link 2: SQL session -> statement -> log/event/metadata/rows -> close ----

    @Test
    public void sqlSession_executesStatementAndFetchesResults() throws Exception {
        KyuubiClient.SessionResponse session = client.createSession(sessionConfigs());
        String sessionHandle = session.getIdentifier();
        assertNotNull("session handle", sessionHandle);
        try {
            KyuubiClient.OperationResponse op =
                    client.executeStatement(sessionHandle, "SELECT 1 AS a, 'kyuubi' AS b", true);
            String opHandle = op.getIdentifier();
            assertNotNull("operation handle", opHandle);

            KyuubiClient.OperationEvent event = awaitOperationTerminal(opHandle, 5 * 60 * 1000L);
            assertTrue(
                    "operation should reach a terminal state, was " + event.getState(),
                    event.isTerminal());

            client.getOperationLog(opHandle, 50); // exercise the operation-log link

            KyuubiClient.ResultSetMetadata md = client.getResultSetMetadata(opHandle);
            assertNotNull("result columns", md.getColumns());
            assertTrue("expected >= 2 columns", md.getColumns().size() >= 2);

            KyuubiClient.RowSetResponse rows =
                    client.getOperationRowSet(opHandle, 100, "FETCH_NEXT");
            assertNotNull("rows", rows.getRows());
            assertTrue("expected at least one row", rows.getRows().size() >= 1);

            client.updateOperation(opHandle, "close");
        } finally {
            client.closeSession(sessionHandle);
        }
    }

    // ---- Link 3: status query + local (submission) log + kill ----

    @Test
    public void statusLocalLogAndKill_onRunningBatch() throws Exception {
        // many slices so the batch is comfortably still running when we kill it
        String batchId = client.submitBatch(sparkPiArgs("200000")).getId();
        assertNotNull("batch id", batchId);

        KyuubiClient.BatchResponse running = awaitState(batchId, "RUNNING", 4 * 60 * 1000L);
        assertNotNull("status response", running.getState());

        KyuubiClient.LogResponse localLog = client.getBatchLogs(batchId, 0, 100);
        assertNotNull("local (submission) log", localLog.getLogRowSet());

        client.killBatch(batchId);
        KyuubiClient.BatchResponse terminal = awaitTerminal(batchId, 90 * 1000L);
        assertTrue(
                "expected a terminal state after kill, was " + terminal.getState(),
                terminal.isFinished());
    }

    // ---- helpers ----

    private SparkSubmitArgs sparkPiArgs(String slices) {
        SparkSubmitArgs args = new SparkSubmitArgs();
        args.setBatchType("SPARK");
        args.setName("e2e-" + slices);
        args.setClassName("org.apache.spark.examples.SparkPi");
        args.setResource(
                env(
                        "KYUUBI_E2E_JAR",
                        "local:///opt/spark/examples/jars/spark-examples_2.12-3.5.7.jar"));
        args.setArgs(Collections.singletonList(slices));
        args.getConf().putAll(k8sConf());
        args.getConf().put("spark.submit.deployMode", "cluster");
        args.getConf().put("spark.executor.instances", "1");
        args.getConf().put("spark.driver.memory", "512m");
        args.getConf().put("spark.executor.memory", "512m");
        return args;
    }

    private Map<String, String> sessionConfigs() {
        // The session's Spark engine launches on Kubernetes with the same cluster confs.
        Map<String, String> c = new HashMap<>(k8sConf());
        c.put("spark.executor.instances", "1");
        c.put("spark.driver.memory", "512m");
        c.put("spark.executor.memory", "512m");
        return c;
    }

    private Map<String, String> k8sConf() {
        Map<String, String> c = new HashMap<>();
        c.put("spark.master", "k8s://https://kubernetes.default.svc");
        c.put("spark.kubernetes.container.image", env("KYUUBI_E2E_IMAGE", "apache/spark:3.5.7"));
        c.put(
                "spark.kubernetes.authenticate.driver.serviceAccountName",
                env("KYUUBI_E2E_SA", "kyuubi"));
        c.put("spark.kubernetes.namespace", env("KYUUBI_E2E_NAMESPACE", "kyuubi"));
        return c;
    }

    private CollectingHandler streamToEnd(String batchId, long budgetMillis) throws Exception {
        CollectingHandler handler = new CollectingHandler();
        long deadline = System.currentTimeMillis() + budgetMillis;
        boolean firstConnect = true;
        while (System.currentTimeMillis() < deadline) {
            int tail = firstConnect ? 200 : 0;
            int since = firstConnect ? 0 : 2;
            firstConnect = false;
            KyuubiClient.DriverLogStreamResult result =
                    client.streamDriverLog(batchId, tail, since, false, a -> {}, handler);
            if (result == KyuubiClient.DriverLogStreamResult.ENDED) {
                return handler;
            }
            if (result == KyuubiClient.DriverLogStreamResult.DISABLED) {
                throw new IllegalStateException("driver log streaming is disabled on " + url);
            }
            if (client.getBatch(batchId).isFinished()) {
                return handler;
            }
            Thread.sleep(2000);
        }
        return handler;
    }

    private KyuubiClient.BatchResponse awaitTerminal(String batchId, long budgetMillis)
            throws Exception {
        long deadline = System.currentTimeMillis() + budgetMillis;
        KyuubiClient.BatchResponse last = client.getBatch(batchId);
        while (!last.isFinished() && System.currentTimeMillis() < deadline) {
            Thread.sleep(2000);
            last = client.getBatch(batchId);
        }
        return last;
    }

    private KyuubiClient.BatchResponse awaitState(String batchId, String state, long budgetMillis)
            throws Exception {
        long deadline = System.currentTimeMillis() + budgetMillis;
        KyuubiClient.BatchResponse last = client.getBatch(batchId);
        while (!state.equals(last.getState())
                && !last.isFinished()
                && System.currentTimeMillis() < deadline) {
            Thread.sleep(2000);
            last = client.getBatch(batchId);
        }
        return last;
    }

    private KyuubiClient.OperationEvent awaitOperationTerminal(String opHandle, long budgetMillis)
            throws Exception {
        long deadline = System.currentTimeMillis() + budgetMillis;
        KyuubiClient.OperationEvent ev = client.getOperationEvent(opHandle);
        while (!ev.isTerminal() && System.currentTimeMillis() < deadline) {
            Thread.sleep(2000);
            ev = client.getOperationEvent(opHandle);
        }
        return ev;
    }

    private static final class CollectingHandler implements KyuubiClient.DriverLogHandler {
        final List<String> lines = new ArrayList<>();
        String endReason;
        String errorMessage;

        @Override
        public void onLog(String line, long timestampMillis) {
            lines.add(line);
        }

        @Override
        public void onEnd(String reason) {
            endReason = reason;
        }

        @Override
        public void onError(String message) {
            errorMessage = message;
        }
    }
}
