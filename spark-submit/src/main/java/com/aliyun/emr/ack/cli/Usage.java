package com.aliyun.emr.ack.cli;

/** The {@code --help} usage text. */
public final class Usage {
    private Usage() {
    }

    public static void print() {
        System.out.println("Spark Submit Client for Kyuubi Server");
        System.out.println("=====================================\n");
        System.out.println("Usage:");
        System.out.println("  spark-submit [options] <app jar | python file> [app arguments]");
        System.out.println("  spark-submit -e <sql-string> [options]");
        System.out.println("  spark-submit -f <sql-file> [options]\n");
        System.out.println("Options:");
        System.out.println("  --class <class name>          Application's main class (required for JAR)");
        System.out.println("  --name <name>                 Name of your application");
        System.out.println("  --num-executors <num>         Number of executors");
        System.out.println("  --driver-cores <cores>        Driver cores");
        System.out.println("  --driver-memory <memory>      Memory for driver (e.g., 1g, 512m)");
        System.out.println("  --executor-cores <cores>      Number of cores per executor");
        System.out.println("  --executor-memory <memory>    Memory per executor (e.g., 1g, 512m)");
        System.out.println("  --files <file1,file2>         Comma-separated files to distribute");
        System.out.println("  --py-files <py1,py2>          Comma-separated py files (PySpark only)");
        System.out.println("  --jars <jar1,jar2>            Comma-separated extra JARs");
        System.out.println("  --archives <a1,a2>            Comma-separated archives");
        System.out.println("  --queue <queueName>           Queue name");
        System.out.println("  --proxy-user <user>           Proxy user (sets hive.server2.proxy.user)");
        System.out.println("  --deploy-mode <mode>          Deploy mode (cluster/client, default: cluster)");
        System.out.println("                                Note: client mode is not supported and will be");
        System.out.println("                                automatically changed to cluster mode");
        System.out.println("  --conf <key>=<value>          Spark configuration property");
        System.out.println("  --status <batchId>            Query batch status");
        System.out.println("  --kill <batchId>              Kill a batch job");
        System.out.println("  --timeout <seconds>           Timeout for job completion in seconds.");
        System.out.println("                                If exceeded, the job will be killed and exit");
        System.out.println("                                with code 124");
        System.out.println("  --no-driver-log               Do not stream the live Spark driver pod log to the");
        System.out.println("                                console (driver log streaming is on by default for");
        System.out.println("                                batch jobs on Kubernetes; requires Kyuubi 1.12+)");
        System.out.println("  --driver-log-grep <regex>     Only print driver log lines matching the regex");
        System.out.println("                                (e.g. 'WARN|ERROR'). Java regex, substring match.");
        System.out.println("  --driver-log-grep-v <regex>   Drop driver log lines matching the regex");
        System.out.println("                                (e.g. 'TaskSetManager' to hide per-task spam)");
        System.out.println("  -e <sql-string>               Execute the given SQL statement (spark-sql mode)");
        System.out.println("  -f <sql-file>                 Execute SQL from the given file (spark-sql mode)");
        System.out.println("  --session                     Use session mode for SQL (-e/-f) instead of default batch mode.");
        System.out.println("                                Session mode returns query results as a table.");
        System.out.println("  --kyuubi-url <url>            Kyuubi server URL (overrides all other config)");
        System.out.println("  --kyuubi-user <user>          Kyuubi username (overrides all other config)");
        System.out.println("  --kyuubi-password <pwd>       Kyuubi password (overrides all other config)");
        System.out.println("  --config-file <path>          Custom config file path");
        System.out.println("  --help, -h                    Show this help message\n");
        System.out.println("Spark SQL Mode:");
        System.out.println("  Use -e or -f to execute SQL statements via Kyuubi session (like spark-sql).");
        System.out.println("  Multiple statements separated by ';' are supported.\n");
        System.out.println("  Examples:");
        System.out.println("    spark-submit -e \"SHOW DATABASES\"");
        System.out.println("    spark-submit -e \"SELECT * FROM my_db.my_table LIMIT 10\"");
        System.out.println("    spark-submit -f /path/to/query.sql");
        System.out.println("    spark-submit -f /path/to/query.sql --conf spark.executor.memory=2g\n");
        System.out.println("Configuration:");
        System.out.println("  Configure Kyuubi server connection via one of the following (priority order):\n");
        System.out.println("  1. Command-line arguments (highest priority):");
        System.out.println("     --kyuubi-url http://your-kyuubi-server:port");
        System.out.println("     --kyuubi-user your-username");
        System.out.println("     --kyuubi-password your-password\n");
        System.out.println("  2. Configuration file:");
        System.out.println("     Create: ~/.spark-submit.conf (or use --config-file <path>)");
        System.out.println("     Content:");
        System.out.println("       kyuubi.server.url=http://your-kyuubi-server:port");
        System.out.println("       kyuubi.server.username=your-username");
        System.out.println("       kyuubi.server.password=your-password");
        System.out.println("       spark.history.server.url=http://your-history-server:port  # Optional\n");
        System.out.println("  3. Environment variables:");
        System.out.println("     export KYUUBI_SERVER_URL=http://your-kyuubi-server:port");
        System.out.println("     export KYUUBI_SERVER_USERNAME=your-username");
        System.out.println("     export KYUUBI_SERVER_PASSWORD=your-password");
        System.out.println("     export SPARK_HISTORY_SERVER_URL=http://your-history-server:port  # Optional\n");
        System.out.println("  4. System properties (lowest priority):");
        System.out.println("     -Dkyuubi.server.url=http://your-kyuubi-server:port");
        System.out.println("     -Dkyuubi.server.username=your-username");
        System.out.println("     -Dkyuubi.server.password=your-password");
        System.out.println("     -Dspark.history.server.url=http://your-history-server:port  # Optional\n");
        System.out.println("Resources:");
        System.out.println("  Recommended: upload JAR to OSS and use oss://bucket/path/app.jar");
        System.out.println("Examples:");
        System.out.println("  spark-submit --name spark-pi \\");
        System.out.println("               --conf spark.submit.deployMode=cluster \\");
        System.out.println("               --class org.apache.spark.examples.SparkPi \\");
        System.out.println("               oss://your-bucket/path/spark-examples_2.12-3.5.7.jar\n");
        System.out.println("  spark-submit --name pyspark-job \\");
        System.out.println("               --py-files oss://your-bucket/lib1.py \\");
        System.out.println("               --files oss://your-bucket/conf.yaml \\");
        System.out.println("               oss://your-bucket/jobs/main.py --arg1 value1\n");
    }
}
