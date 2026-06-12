package com.aliyun.emr.ack;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeNotNull;

import com.aliyun.emr.ack.CliRunner.Result;
import com.aliyun.emr.ack.cli.SparkSubmitArgs;
import com.aliyun.emr.ack.client.Config;
import com.aliyun.emr.ack.client.KyuubiClient;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

/**
 * End-to-end coverage of the full CLI orchestration. Unlike {@link KyuubiE2ETest} (which drives the
 * REST client directly), this test invokes the real {@link SparkSubmit#main(String[])} in-process
 * against a live cluster, so the actual monitoring loops, the live {@code DriverLogStreamer}, all
 * three run modes, the console reporting, and the {@code --status}/{@code --kill} paths execute and
 * are measured by JaCoCo — the half of the codebase that {@code System.exit} otherwise hides.
 *
 * <p>The in-process {@code main()} invocation and its {@code System.exit} trap live in {@link
 * CliRunner}.
 *
 * <p>Opt-in: skipped unless {@code KYUUBI_E2E_URL} is set.
 */
public class SparkSubmitCliE2ETest {

    private String url;
    private String user;
    private KyuubiClient client; // only to create/clean up kill/status targets

    private static String env(String key, String dflt) {
        String v = System.getenv(key);
        return (v == null || v.isEmpty()) ? dflt : v;
    }

    @Before
    public void setUp() {
        url = System.getenv("KYUUBI_E2E_URL");
        assumeNotNull(url);
        user = env("KYUUBI_E2E_USER", "e2e");

        SparkSubmitArgs conn = new SparkSubmitArgs();
        conn.setKyuubiUrl(url);
        conn.setKyuubiUser(user);
        Config config = new Config("/tmp/__cli_e2e_nonexistent.conf");
        config.applyOverrides(conn);
        client = new KyuubiClient(config);
    }

    // ---- jar submit: full monitor loop + live DriverLogStreamer + success ----

    @Test
    public void jarSubmit_streamsDriverLogAndCompletes() {
        // enough slices that the job runs long enough for the live stream to engage before it ends
        Result r = run(jarArgs("e2e-cli-stream", 200));
        assertEquals("exit code (stderr=" + tail(r.err) + ")", 0, r.code);
        assertTrue("driver log header", r.out.contains("=== Driver Log (streaming) ==="));
        assertTrue("Pi result", r.out.contains("Pi is roughly"));
        assertTrue("success line", r.out.contains("Job completed successfully"));
    }

    // ---- jar submit with --no-driver-log: the localLog branch of the monitor loop ----

    @Test
    public void jarSubmit_noDriverLog_usesSubmissionLog() {
        List<String> args = jarArgs("e2e-cli-nolog", 50);
        args.add("--no-driver-log");
        Result r = run(args);
        assertEquals("exit code (stderr=" + tail(r.err) + ")", 0, r.code);
        assertFalse("must not stream driver log", r.out.contains("=== Driver Log (streaming) ==="));
        assertTrue("success line", r.out.contains("Job completed successfully"));
    }

    // ---- jar submit with a grep-v filter: the filter drop path inside the streamer ----

    @Test
    public void jarSubmit_driverLogGrepV_dropsMatchingLines() {
        List<String> args = jarArgs("e2e-cli-filter", 200);
        args.add("--driver-log-grep-v");
        args.add("TaskSetManager");
        Result r = run(args);
        assertEquals("exit code (stderr=" + tail(r.err) + ")", 0, r.code);
        assertTrue("driver log header", r.out.contains("=== Driver Log (streaming) ==="));
        // everything between the streaming header and the final summary must be free of the pattern
        String streamed = between(r.out, "=== Driver Log (streaming) ===", "Job finished!");
        assertFalse("filtered pattern leaked into the stream", streamed.contains("TaskSetManager"));
    }

    // ---- SQL session mode: executeSqlMode + single statement + result table ----

    @Test
    public void sqlSessionMode_printsResultTable() {
        List<String> args = baseArgs("e2e-cli-sql-session");
        args.add("-e");
        args.add("SELECT 1 AS a, 'kyuubi' AS b");
        args.add("--session");
        Result r = run(args);
        assertEquals("exit code (stderr=" + tail(r.err) + ")", 0, r.code);
        assertTrue("result table footer", r.out.contains("row(s) in set"));
    }

    @Test
    public void sqlSessionMode_runsMultipleStatements() {
        List<String> args = baseArgs("e2e-cli-sql-session-multi");
        args.add("-e");
        args.add("SELECT 1 AS first_col; SELECT 2 AS second_col");
        args.add("--session");
        Result r = run(args);
        assertEquals("exit code (stderr=" + tail(r.err) + ")", 0, r.code);
        assertTrue("first statement ran", r.out.contains("[1/2]"));
        assertTrue("second statement ran", r.out.contains("[2/2]"));
        assertTrue("success summary", r.out.contains("All SQL statements completed successfully"));
        assertTrue("result tables", countOccurrences(r.out, "row(s) in set") >= 2);
    }

    @Test
    public void sqlSessionMode_badSqlExits1AndClosesSession() {
        List<String> args = baseArgs("e2e-cli-sql-session-fail");
        args.add("-e");
        args.add("SELECT * FROM table_that_should_not_exist_for_e2e");
        args.add("--session");
        Result r = run(args);
        assertEquals("exit code (stderr=" + tail(r.err) + ")", 1, r.code);
        assertTrue(
                "reported statement error",
                r.err.contains("Error executing statement")
                        || r.err.contains("TABLE_OR_VIEW_NOT_FOUND"));
        assertTrue(
                "attempted session close",
                r.out.contains("Closing session:") || r.err.contains("Closing session"));
    }

    // ---- SQL batch mode: executeSqlBatchMode (the second monitor loop) ----

    @Test
    public void sqlBatchMode_runsThroughBatchOrchestration() {
        List<String> args = baseArgs("e2e-cli-sql-batch");
        args.add("-e");
        args.add("SELECT 1");
        Result r = run(args); // default (non-session) => SparkSQLCLIDriver batch
        // The orchestration must run end to end; on images without spark-sql-cli.jar the batch
        // may end in ERROR, which still exercises the batch monitor + failure path. Either way the
        // process must terminate cleanly (not hang or throw).
        assertTrue("clean terminal exit, was " + r.code, r.code == 0 || r.code == 1);
        assertTrue("batch-mode banner", r.out.contains("Submitting Spark SQL Batch Job"));
        assertTrue(
                "reached a terminal state",
                r.out.contains("Job finished!") || r.out.contains("Job failed"));
    }

    // ---- status query + kill (quick paths of main) ----

    @Test
    public void statusAndKill_onRunningBatch() throws Exception {
        SparkSubmitArgs longJob = new SparkSubmitArgs();
        longJob.setBatchType("SPARK");
        longJob.setName("e2e-cli-killtarget");
        longJob.setClassName("org.apache.spark.examples.SparkPi");
        longJob.setResource(jar());
        longJob.setArgs(Collections.singletonList("200000"));
        longJob.getConf().putAll(k8sConfArgsAsMap());
        longJob.getConf().put("spark.submit.deployMode", "cluster");
        longJob.getConf().put("spark.executor.instances", "1");
        String batchId = client.submitBatch(longJob).getId();
        assertNotNull(batchId);
        try {
            Result status = run(connArgs("--status", batchId));
            assertEquals("status exit", 0, status.code);
            assertTrue("status output", status.out.contains("State:"));

            Result kill = run(connArgs("--kill", batchId));
            assertEquals("kill exit", 0, kill.code);
            assertTrue("kill output", kill.out.contains("Kill request sent"));
        } finally {
            try {
                client.killBatch(batchId);
            } catch (Exception ignore) {
                // best-effort cleanup
            }
        }
    }

    // ---- timeout path: the monitor loop hits --timeout, kills, and exits 124 ----

    @Test
    public void jarSubmit_timeoutKillsAndExits124() {
        List<String> args = jarArgs("e2e-cli-timeout", 200000); // far too long to finish
        args.add("--timeout");
        args.add("20"); // a later --timeout wins over the default, forcing the timeout branch
        Result r = run(args);
        assertEquals("timeout exit code (stderr=" + tail(r.err) + ")", 124, r.code);
        assertTrue(
                "timeout message", r.err.contains("Job timeout") || r.out.contains("Job timeout"));
    }

    // ---- failure path: a bad main class drives the batch to ERROR and exit 1 ----

    @Test
    public void jarSubmit_failureExits1AndReportsError() {
        List<String> args = baseArgs("e2e-cli-fail");
        args.add("--class");
        args.add("com.example.DoesNotExistMainClass");
        args.add(jar());
        args.add("1");
        Result r = run(args);
        assertEquals("failure exit code (stderr=" + tail(r.err) + ")", 1, r.code);
        assertTrue("failure report", r.out.contains("Job failed") || r.out.contains("❌"));
    }

    // ---- large SQL (>10KB) upload via the server plugin, then SQL batch orchestration ----

    @Test
    public void sqlBatchMode_largeSqlUploadsViaServerPlugin() throws Exception {
        // >10KB forces the toolkit's upload path: client.uploadFile -> server /files/upload -> URI
        char[] pad = new char[12000];
        java.util.Arrays.fill(pad, 'x');
        String sql = "-- large-sql upload e2e\n-- " + new String(pad) + "\nSELECT 1;\n";
        java.io.File f = java.io.File.createTempFile("e2e-large", ".sql");
        f.deleteOnExit();
        try (java.io.FileWriter w = new java.io.FileWriter(f)) {
            w.write(sql);
        }

        List<String> args = baseArgs("e2e-cli-upload");
        args.add("-f");
        args.add(f.getAbsolutePath());
        Result r = run(args); // default (non-session) => SQL batch mode, triggers the upload
        assertTrue(
                "expected a server-side upload (stderr=" + tail(r.err) + ")",
                r.err.contains("uploaded via Kyuubi server") || r.err.contains("s3a://"));
        assertTrue("clean terminal exit, was " + r.code, r.code == 0 || r.code == 1);
    }

    // ---- fast CLI paths that need no cluster (validation / usage), still driven through main()
    // ----

    @Test
    public void help_printsUsage() {
        Result r = run(new String[] {"--help"});
        assertEquals(0, r.code);
        assertTrue("usage banner", r.out.contains("Usage:"));
        assertTrue("documents driver-log flags", r.out.contains("--no-driver-log"));
    }

    @Test
    public void statusAndKillTogether_isRejected() {
        Result r = run(connArgs("--status", "a", "--kill", "b"));
        assertEquals(1, r.code);
        assertTrue(r.err.contains("cannot be used together"));
    }

    @Test
    public void badDriverLogRegex_failsBeforeSubmit() {
        Result r = run(connArgs("--driver-log-grep", "(", "--class", "X", "x.jar"));
        assertEquals(1, r.code);
        assertTrue("invalid-regex error", r.err.contains("Invalid regex"));
    }

    // ---- argument builders ----

    private String jar() {
        return env(
                "KYUUBI_E2E_JAR", "local:///opt/spark/examples/jars/spark-examples_2.12-3.5.7.jar");
    }

    private String[] connArgs(String... extra) {
        List<String> a = new ArrayList<>();
        a.add("--kyuubi-url");
        a.add(url);
        a.add("--kyuubi-user");
        a.add(user);
        Collections.addAll(a, extra);
        return a.toArray(new String[0]);
    }

    private List<String> baseArgs(String name) {
        List<String> a = new ArrayList<>();
        a.add("--kyuubi-url");
        a.add(url);
        a.add("--kyuubi-user");
        a.add(user);
        a.add("--name");
        a.add(name);
        a.add("--timeout");
        a.add("300");
        a.add("--conf");
        a.add("spark.master=k8s://https://kubernetes.default.svc");
        a.add("--conf");
        a.add("spark.kubernetes.container.image=" + env("KYUUBI_E2E_IMAGE", "apache/spark:3.5.7"));
        a.add("--conf");
        a.add(
                "spark.kubernetes.authenticate.driver.serviceAccountName="
                        + env("KYUUBI_E2E_SA", "kyuubi"));
        a.add("--conf");
        a.add("spark.kubernetes.namespace=" + env("KYUUBI_E2E_NAMESPACE", "kyuubi"));
        a.add("--conf");
        a.add("spark.executor.instances=1");
        a.add("--conf");
        a.add("spark.driver.memory=512m");
        a.add("--conf");
        a.add("spark.executor.memory=512m");
        return a;
    }

    private List<String> jarArgs(String name, int slices) {
        List<String> a = baseArgs(name);
        a.add("--class");
        a.add("org.apache.spark.examples.SparkPi");
        a.add(jar());
        a.add(Integer.toString(slices));
        return a;
    }

    private java.util.Map<String, String> k8sConfArgsAsMap() {
        java.util.Map<String, String> c = new java.util.HashMap<>();
        c.put("spark.master", "k8s://https://kubernetes.default.svc");
        c.put("spark.kubernetes.container.image", env("KYUUBI_E2E_IMAGE", "apache/spark:3.5.7"));
        c.put(
                "spark.kubernetes.authenticate.driver.serviceAccountName",
                env("KYUUBI_E2E_SA", "kyuubi"));
        c.put("spark.kubernetes.namespace", env("KYUUBI_E2E_NAMESPACE", "kyuubi"));
        return c;
    }

    // ---- in-process main() runner (shared harness in CliRunner) ----

    private Result run(List<String> args) {
        return CliRunner.run(args.toArray(new String[0]));
    }

    private Result run(String[] args) {
        return CliRunner.run(args);
    }

    private static String tail(String s) {
        return CliRunner.tail(s);
    }

    private static String between(String s, String start, String end) {
        return CliRunner.between(s, start, end);
    }

    private static int countOccurrences(String s, String needle) {
        int count = 0;
        int i = 0;
        while ((i = s.indexOf(needle, i)) >= 0) {
            count++;
            i += needle.length();
        }
        return count;
    }
}
