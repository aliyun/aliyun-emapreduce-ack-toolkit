package com.aliyun.emr.ack.util;

import com.aliyun.emr.ack.client.KyuubiClient;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/** Console presentation helpers: timestamps, durations, SQL display formatting and result tables. */
public final class Console {
    private Console() {
    }

    public static String timestamp() {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
    }

    public static String formatDuration(long totalSeconds) {
        long hours = totalSeconds / 3600;
        long minutes = (totalSeconds % 3600) / 60;
        long seconds = totalSeconds % 60;
        if (hours > 0) return String.format("%dh %dm %ds", hours, minutes, seconds);
        if (minutes > 0) return String.format("%dm %ds", minutes, seconds);
        return String.format("%ds", seconds);
    }

    /** Collapse whitespace and clip a SQL string for one-line display. */
    public static String truncateSql(String sql, int maxLen) {
        String oneLine = sql.replaceAll("\\s+", " ").trim();
        if (oneLine.length() > maxLen) {
            return oneLine.substring(0, maxLen) + "...";
        }
        return oneLine;
    }

    public static String extractFirstLine(String text) {
        if (text == null) return "";
        int newline = text.indexOf('\n');
        return newline >= 0 ? text.substring(0, newline) : text;
    }

    public static String padRight(String s, int width) {
        if (s.length() >= width) return s;
        StringBuilder sb = new StringBuilder(s);
        for (int i = s.length(); i < width; i++) {
            sb.append(' ');
        }
        return sb.toString();
    }

    /** Print a result set as a bordered table (spark-sql style), followed by a row count. */
    public static void printResultTable(List<KyuubiClient.ColumnDesc> columns, List<List<String>> rows) {
        int numCols = columns.size();

        // Calculate column widths
        int[] widths = new int[numCols];
        for (int i = 0; i < numCols; i++) {
            widths[i] = columns.get(i).getColumnName().length();
        }
        for (List<String> row : rows) {
            for (int i = 0; i < numCols && i < row.size(); i++) {
                widths[i] = Math.max(widths[i], row.get(i).length());
            }
        }

        // Build separator line
        StringBuilder separator = new StringBuilder("+");
        for (int w : widths) {
            for (int j = 0; j < w + 2; j++) {
                separator.append("-");
            }
            separator.append("+");
        }
        String sep = separator.toString();

        // Print header
        System.out.println(sep);
        StringBuilder header = new StringBuilder("|");
        for (int i = 0; i < numCols; i++) {
            header.append(" ").append(padRight(columns.get(i).getColumnName(), widths[i])).append(" |");
        }
        System.out.println(header.toString());
        System.out.println(sep);

        // Print rows
        for (List<String> row : rows) {
            StringBuilder rowLine = new StringBuilder("|");
            for (int i = 0; i < numCols; i++) {
                String value = i < row.size() ? row.get(i) : "";
                rowLine.append(" ").append(padRight(value, widths[i])).append(" |");
            }
            System.out.println(rowLine.toString());
        }
        System.out.println(sep);

        // Print row count
        System.out.println(rows.size() + " row(s) in set");
    }
}
