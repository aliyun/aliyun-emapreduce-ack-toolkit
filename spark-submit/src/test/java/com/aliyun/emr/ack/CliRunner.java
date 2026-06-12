package com.aliyun.emr.ack;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.security.Permission;

/**
 * Drives {@link SparkSubmit#main(String[])} in-process and captures its exit code, stdout and
 * stderr so the CLI orchestration can be asserted on.
 *
 * <p>{@code main()} terminates via {@code System.exit}. A {@link SecurityManager} converts that into
 * a catchable {@link ExitInvoked}, which extends {@link Error} so the broad {@code catch (Exception)}
 * blocks inside {@code main()} never swallow it. Java 11 (the project runtime) supports this.
 *
 * <p>Shared by the offline validation tests ({@link SparkSubmitCliTest}, no cluster) and the
 * cluster-backed {@link SparkSubmitCliE2ETest}.
 */
final class CliRunner {

    private CliRunner() {
    }

    /** Run {@code SparkSubmit.main(args)} with stdout/stderr/exit captured. */
    static Result run(String... args) {
        PrintStream origOut = System.out;
        PrintStream origErr = System.err;
        SecurityManager origSm = System.getSecurityManager();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        int code = Integer.MIN_VALUE;
        try {
            System.setOut(new PrintStream(out, true, "UTF-8"));
            System.setErr(new PrintStream(err, true, "UTF-8"));
            System.setSecurityManager(new NoExitSecurityManager());
            SparkSubmit.main(args);
        } catch (ExitInvoked e) {
            code = e.status;
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        } finally {
            System.setSecurityManager(origSm);
            System.setOut(origOut);
            System.setErr(origErr);
        }
        return new Result(code, asString(out), asString(err));
    }

    private static String asString(ByteArrayOutputStream b) {
        return new String(b.toByteArray(), StandardCharsets.UTF_8);
    }

    /** Last up-to-400 chars of a stream, for terse failure messages. */
    static String tail(String s) {
        if (s == null) {
            return "";
        }
        return s.length() <= 400 ? s : s.substring(s.length() - 400);
    }

    /** Substring of {@code s} between the first {@code start} and the following {@code end}. */
    static String between(String s, String start, String end) {
        int i = s.indexOf(start);
        if (i < 0) {
            return "";
        }
        int j = s.indexOf(end, i);
        return j < 0 ? s.substring(i) : s.substring(i, j);
    }

    static final class Result {
        final int code;
        final String out;
        final String err;

        Result(int code, String out, String err) {
            this.code = code;
            this.out = out;
            this.err = err;
        }
    }

    /** Thrown in place of a real {@code System.exit}; extends Error so {@code catch (Exception)} ignores it. */
    private static final class ExitInvoked extends Error {
        final int status;

        ExitInvoked(int status) {
            this.status = status;
        }
    }

    private static final class NoExitSecurityManager extends SecurityManager {
        @Override
        public void checkExit(int status) {
            throw new ExitInvoked(status);
        }

        @Override
        public void checkPermission(Permission perm) {
            // allow everything else (no extra restrictions during the test)
        }

        @Override
        public void checkPermission(Permission perm, Object context) {
            // allow everything else
        }
    }
}
