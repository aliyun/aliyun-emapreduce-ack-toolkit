package com.aliyun.emr.ack;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Offline coverage of every {@link SparkSubmit#main(String[])} path that resolves <em>before</em> the
 * first network call: usage/help, the mutually-exclusive argument guards, regex validation and the
 * required-argument checks. None of these need a cluster, so unlike {@link SparkSubmitCliE2ETest}
 * they run on every build and form the regression net that protects the CLI surface during refactors.
 *
 * <p>A dummy {@code --kyuubi-url} is supplied where the path runs past config loading; it is never
 * contacted because each case exits at a validation guard first.
 */
public class SparkSubmitCliTest {

    private static final String DUMMY_URL = "http://127.0.0.1:1";

    // ---- usage / help ----

    @Test
    public void noArgs_printsUsageAndExitsZero() {
        CliRunner.Result r = CliRunner.run();
        assertEquals(0, r.code);
        assertTrue("usage banner", r.out.contains("Usage:"));
    }

    @Test
    public void helpLong_printsUsage() {
        CliRunner.Result r = CliRunner.run("--help");
        assertEquals(0, r.code);
        assertTrue("usage banner", r.out.contains("Usage:"));
        assertTrue("documents driver-log flags", r.out.contains("--no-driver-log"));
        assertTrue("documents grep flags", r.out.contains("--driver-log-grep"));
    }

    @Test
    public void helpShort_printsUsage() {
        CliRunner.Result r = CliRunner.run("-h");
        assertEquals(0, r.code);
        assertTrue("usage banner", r.out.contains("Usage:"));
    }

    // ---- mutually exclusive operations ----

    @Test
    public void statusAndKillTogether_isRejected() {
        CliRunner.Result r = CliRunner.run("--kyuubi-url", DUMMY_URL, "--status", "a", "--kill", "b");
        assertEquals(1, r.code);
        assertTrue("error mentions the conflict (" + CliRunner.tail(r.err) + ")",
                r.err.contains("cannot be used together"));
    }

    @Test
    public void sqlFileAndStatementTogether_isRejected() {
        CliRunner.Result r = CliRunner.run(
                "--kyuubi-url", DUMMY_URL, "-f", "/tmp/q.sql", "-e", "SELECT 1");
        assertEquals(1, r.code);
        assertTrue("error mentions -f/-e conflict (" + CliRunner.tail(r.err) + ")",
                r.err.contains("-f and -e cannot be used together"));
    }

    // ---- driver-log regex validation (fails fast, before submit) ----

    @Test
    public void badDriverLogGrep_isRejected() {
        CliRunner.Result r = CliRunner.run("--kyuubi-url", DUMMY_URL, "--driver-log-grep", "(");
        assertEquals(1, r.code);
        assertTrue("invalid-regex error names the flag (" + CliRunner.tail(r.err) + ")",
                r.err.contains("Invalid regex") && r.err.contains("--driver-log-grep"));
    }

    @Test
    public void badDriverLogGrepV_isRejected() {
        CliRunner.Result r = CliRunner.run("--kyuubi-url", DUMMY_URL, "--driver-log-grep-v", "[");
        assertEquals(1, r.code);
        assertTrue("invalid-regex error names the grep-v flag (" + CliRunner.tail(r.err) + ")",
                r.err.contains("Invalid regex") && r.err.contains("--driver-log-grep-v"));
    }

    // ---- required arguments for a jar submission ----

    @Test
    public void missingResource_isRejected() {
        CliRunner.Result r = CliRunner.run("--kyuubi-url", DUMMY_URL, "--class", "com.example.Main");
        assertEquals(1, r.code);
        assertTrue("error mentions the missing resource (" + CliRunner.tail(r.err) + ")",
                r.err.contains("Resource") && r.err.contains("required"));
    }

    @Test
    public void missingClass_forJar_isRejected() {
        CliRunner.Result r = CliRunner.run("--kyuubi-url", DUMMY_URL, "app.jar");
        assertEquals(1, r.code);
        assertTrue("error mentions the missing class (" + CliRunner.tail(r.err) + ")",
                r.err.contains("--class is required"));
    }
}
