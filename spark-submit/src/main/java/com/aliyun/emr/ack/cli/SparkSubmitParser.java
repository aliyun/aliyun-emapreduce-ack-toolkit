package com.aliyun.emr.ack.cli;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Parser for spark-submit command line arguments.
 *
 * <p>The toolkit is a drop-in replacement for {@code spark-submit}, so it must reproduce its CLI
 * semantics exactly: unknown options are tolerated silently, every token after the resource jar is
 * passed through verbatim as an application argument, {@code --conf k=v} is repeatable and the
 * list-valued options are comma-separated. Rather than a long {@code if/else} chain, the recognised
 * options live in two declarative tables — {@link #VALUE_OPTS} (consume the next token) and
 * {@link #FLAG_OPTS} (no value) — so adding an option is a single table entry. Handlers mutate the
 * result's already-initialised collections directly.
 */
public class SparkSubmitParser {

    /** Options that consume the following token; the handler applies it to the parsed result. */
    private static final Map<String, BiConsumer<SparkSubmitArgs, String>> VALUE_OPTS = new HashMap<>();

    /** Boolean flags that take no value. */
    private static final Map<String, Consumer<SparkSubmitArgs>> FLAG_OPTS = new HashMap<>();

    static {
        // value -> a field on the result
        VALUE_OPTS.put("--name", SparkSubmitArgs::setName);
        VALUE_OPTS.put("--status", SparkSubmitArgs::setStatusBatchId);
        VALUE_OPTS.put("--kill", SparkSubmitArgs::setKillBatchId);
        VALUE_OPTS.put("--class", SparkSubmitArgs::setClassName);
        VALUE_OPTS.put("-c", SparkSubmitArgs::setClassName);
        VALUE_OPTS.put("--proxy-user", SparkSubmitArgs::setProxyUser);
        VALUE_OPTS.put("--deploy-mode", SparkSubmitArgs::setDeployMode);
        VALUE_OPTS.put("-f", SparkSubmitArgs::setSqlFile);
        VALUE_OPTS.put("-e", SparkSubmitArgs::setSqlStatement);
        VALUE_OPTS.put("--driver-log-grep", SparkSubmitArgs::setDriverLogGrep);
        VALUE_OPTS.put("--driver-log-grep-v", SparkSubmitArgs::setDriverLogGrepV);
        VALUE_OPTS.put("--kyuubi-url", SparkSubmitArgs::setKyuubiUrl);
        VALUE_OPTS.put("--kyuubi-user", SparkSubmitArgs::setKyuubiUser);
        VALUE_OPTS.put("--kyuubi-password", SparkSubmitArgs::setKyuubiPassword);
        VALUE_OPTS.put("--history-url", SparkSubmitArgs::setSparkHistoryUrl);
        VALUE_OPTS.put("--config-file", SparkSubmitArgs::setConfigFile);

        // value -> a single Spark conf key
        VALUE_OPTS.put("--driver-memory", conf("spark.driver.memory"));
        VALUE_OPTS.put("--driver-class-path", conf("spark.driver.extraClassPath"));
        VALUE_OPTS.put("--driver-java-options", conf("spark.driver.extraJavaOptions"));
        VALUE_OPTS.put("--driver-library-path", conf("spark.driver.extraLibraryPath"));
        VALUE_OPTS.put("--executor-memory", conf("spark.executor.memory"));
        VALUE_OPTS.put("--executor-cores", conf("spark.executor.cores"));
        VALUE_OPTS.put("--num-executors", conf("spark.executor.instances"));
        VALUE_OPTS.put("--total-executor-cores", conf("spark.cores.max"));

        // value -> a field AND a conf key
        VALUE_OPTS.put("--driver-cores", (a, v) -> {
            a.setDriverCores(v);
            a.getConf().put("spark.driver.cores", v);
        });
        VALUE_OPTS.put("--queue", (a, v) -> {
            a.setQueue(v);
            a.getConf().put("spark.yarn.queue", v);
        });

        // comma-separated lists
        VALUE_OPTS.put("--jars", csv(SparkSubmitArgs::getJars));
        VALUE_OPTS.put("--packages", csv(SparkSubmitArgs::getPackages));
        VALUE_OPTS.put("--repositories", csv(SparkSubmitArgs::getRepositories));
        VALUE_OPTS.put("--py-files", csv(SparkSubmitArgs::getPyFiles));
        VALUE_OPTS.put("--files", csv(SparkSubmitArgs::getFiles));
        VALUE_OPTS.put("--archives", csv(SparkSubmitArgs::getArchives));

        // options needing custom parsing
        VALUE_OPTS.put("--conf", SparkSubmitParser::putConf);
        VALUE_OPTS.put("--timeout", SparkSubmitParser::parseTimeout);
        // accepted for spark-submit compatibility; the file itself is not consumed yet
        VALUE_OPTS.put("--properties-file", (a, v) -> { });

        // boolean flags
        FLAG_OPTS.put("--session", a -> a.setSqlSessionMode(true));
        FLAG_OPTS.put("--driver-log", a -> a.setDriverLogStream(true));
        FLAG_OPTS.put("--no-driver-log", a -> a.setDriverLogStream(false));
    }

    public static SparkSubmitArgs parse(String[] args) {
        SparkSubmitArgs result = new SparkSubmitArgs();

        int i = 0;
        while (i < args.length) {
            String arg = args[i];

            BiConsumer<SparkSubmitArgs, String> valueOpt = VALUE_OPTS.get(arg);
            Consumer<SparkSubmitArgs> flagOpt;
            if (valueOpt != null) {
                // consume the next token, if present; a trailing option without a value is a no-op
                if (i + 1 < args.length) {
                    valueOpt.accept(result, args[++i]);
                }
            } else if ((flagOpt = FLAG_OPTS.get(arg)) != null) {
                flagOpt.accept(result);
            } else if (arg.startsWith("--")) {
                // unknown option: tolerate it, swallowing a following value-looking token
                if (i + 1 < args.length && !args[i + 1].startsWith("-")) {
                    i++;
                }
            } else if (result.getResource() == null && isResource(arg)) {
                result.setResource(arg);
            } else {
                // the resource jar/script, then everything after it, is a passthrough app argument
                result.getArgs().add(arg);
            }
            i++;
        }

        // Infer batch type from the resource extension
        if (result.getResource() != null && result.getResource().endsWith(".py")) {
            result.setBatchType("PYSPARK");
        } else {
            result.setBatchType("SPARK");
        }

        return result;
    }

    /** A handler that stores the value under a single Spark conf key. */
    private static BiConsumer<SparkSubmitArgs, String> conf(String key) {
        return (a, v) -> a.getConf().put(key, v);
    }

    /** A handler that splits a comma-separated value into the given list. */
    private static BiConsumer<SparkSubmitArgs, String> csv(Function<SparkSubmitArgs, List<String>> listOf) {
        return (a, v) -> {
            for (String part : v.split(",")) {
                String trimmed = part.trim();
                if (!trimmed.isEmpty()) {
                    listOf.apply(a).add(trimmed);
                }
            }
        };
    }

    /** Parse a {@code key=value} pair (split on the first {@code =}) into the conf map. */
    private static void putConf(SparkSubmitArgs a, String confValue) {
        int eqIndex = confValue.indexOf('=');
        if (eqIndex > 0) {
            a.getConf().put(confValue.substring(0, eqIndex), confValue.substring(eqIndex + 1));
        }
    }

    /** Parse the timeout in seconds; an invalid value is warned about and ignored, never fatal. */
    private static void parseTimeout(SparkSubmitArgs a, String value) {
        try {
            a.setTimeoutSeconds(Long.parseLong(value));
        } catch (NumberFormatException e) {
            System.err.println("Warning: Invalid timeout value '" + value + "', ignoring");
        }
    }

    /** Whether a bare positional token is the application resource (jar/python file). */
    private static boolean isResource(String arg) {
        return arg.endsWith(".jar") || arg.endsWith(".py")
                || arg.startsWith("local://") || arg.startsWith("oss://");
    }
}
