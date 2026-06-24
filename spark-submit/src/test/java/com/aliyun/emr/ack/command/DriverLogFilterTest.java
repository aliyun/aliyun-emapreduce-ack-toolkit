package com.aliyun.emr.ack.command;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

public class DriverLogFilterTest {

    private static final String TASK_LINE =
            "26/06/12 03:20:02 INFO TaskSetManager: Finished task 1.0 in stage 0.0";
    private static final String WARN_LINE = "26/06/12 03:20:02 WARN SparkContext: something odd";
    private static final String INFO_LINE = "26/06/12 03:20:02 INFO DAGScheduler: Job 0 finished";

    @Test
    public void allowAll_keepsEveryLine() {
        assertTrue(DriverLogFilter.ALLOW_ALL.shouldPrint(TASK_LINE));
        assertTrue(DriverLogFilter.ALLOW_ALL.shouldPrint(WARN_LINE));
    }

    @Test
    public void noPatterns_keepsEveryLine() {
        DriverLogFilter f = DriverLogFilter.fromRegexes(null, null);
        assertTrue(f.shouldPrint(TASK_LINE));
        f = DriverLogFilter.fromRegexes("", "");
        assertTrue(f.shouldPrint(TASK_LINE));
    }

    @Test
    public void include_keepsOnlyMatchingLines() {
        DriverLogFilter f = DriverLogFilter.fromRegexes("WARN|ERROR", null);
        assertTrue(f.shouldPrint(WARN_LINE));
        assertFalse(f.shouldPrint(INFO_LINE));
        assertFalse(f.shouldPrint(TASK_LINE));
    }

    @Test
    public void exclude_dropsMatchingLines() {
        DriverLogFilter f = DriverLogFilter.fromRegexes(null, "TaskSetManager");
        assertFalse(f.shouldPrint(TASK_LINE));
        assertTrue(f.shouldPrint(WARN_LINE));
        assertTrue(f.shouldPrint(INFO_LINE));
    }

    @Test
    public void includeThenExclude_bothApply() {
        // keep INFO lines, but still drop the per-task spam
        DriverLogFilter f = DriverLogFilter.fromRegexes("INFO", "TaskSetManager");
        assertTrue(f.shouldPrint(INFO_LINE)); // INFO and not TaskSetManager
        assertFalse(f.shouldPrint(TASK_LINE)); // INFO but excluded
        assertFalse(f.shouldPrint(WARN_LINE)); // not INFO
    }

    @Test
    public void substringSemantics_matchAnywhereInLine() {
        DriverLogFilter f = DriverLogFilter.fromRegexes("DAGScheduler", null);
        assertTrue(f.shouldPrint(INFO_LINE)); // pattern is mid-line, not anchored
    }

    @Test
    public void badRegex_failsFastWithFlagNameInMessage() {
        try {
            DriverLogFilter.fromRegexes("(", null);
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("--driver-log-grep"));
        }
        try {
            DriverLogFilter.fromRegexes(null, "[unclosed");
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("--driver-log-grep-v"));
        }
    }

    @Test
    public void compile_returnsNullForBlank() {
        assertEquals(null, DriverLogFilter.compile("x", null));
        assertEquals(null, DriverLogFilter.compile("x", ""));
    }
}
