package com.aliyun.emr.ack.command;

import com.aliyun.emr.ack.cli.*;
import com.aliyun.emr.ack.client.*;
import com.aliyun.emr.ack.util.*;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Client-side line filter for the streamed Spark driver log. A line is kept when it matches the
 * {@code include} pattern (or there is none) AND does not match the {@code exclude} pattern.
 * Matching is substring ({@link java.util.regex.Matcher#find()}) semantics, like grep.
 *
 * <p>Driven by {@code --driver-log-grep} / {@code --driver-log-grep-v} (or their
 * {@code spark.submit.driver.log.grep[-v]} config-file equivalents).
 */
public final class DriverLogFilter {

    /** A filter that keeps every line. */
    public static final DriverLogFilter ALLOW_ALL = new DriverLogFilter(null, null);

    private final Pattern include;
    private final Pattern exclude;

    DriverLogFilter(Pattern include, Pattern exclude) {
        this.include = include;
        this.exclude = exclude;
    }

    /**
     * Build a filter from raw regex strings; null or empty means "no pattern".
     *
     * @throws IllegalArgumentException if either regex is invalid, with a message naming the
     *         offending option so the caller can surface it before submitting a job
     */
    public static DriverLogFilter fromRegexes(String includeRegex, String excludeRegex) {
        return new DriverLogFilter(
                compile("--driver-log-grep", includeRegex),
                compile("--driver-log-grep-v", excludeRegex));
    }

    /** Compile a single filter regex, or return null when unset. Visible for validation/testing. */
    static Pattern compile(String label, String regex) {
        if (regex == null || regex.isEmpty()) {
            return null;
        }
        try {
            return Pattern.compile(regex);
        } catch (PatternSyntaxException e) {
            throw new IllegalArgumentException(
                    "Invalid regex for " + label + " (" + regex + "): " + e.getDescription());
        }
    }

    /** Whether the given driver log line should be printed under this filter. */
    public boolean shouldPrint(String line) {
        if (include != null && !include.matcher(line).find()) {
            return false;
        }
        if (exclude != null && exclude.matcher(line).find()) {
            return false;
        }
        return true;
    }
}
