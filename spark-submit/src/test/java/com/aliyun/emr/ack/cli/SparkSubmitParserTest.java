package com.aliyun.emr.ack.cli;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Exhaustive, offline coverage of {@link SparkSubmitParser}. This is the executable contract for the
 * toolkit's spark-submit-compatible CLI surface: every flag, the comma-split list options, the
 * {@code --conf k=v} form, positional resource detection, the silently-tolerated unknown options and
 * the pass-through application arguments. Any future parser rewrite must keep these green.
 */
public class SparkSubmitParserTest {

    // ---- class / resource / app args ----

    @Test
    public void parsesClassResourceAndAppArgs() {
        SparkSubmitArgs a = SparkSubmitParser.parse(new String[] {
                "--class", "com.example.Main",
                "oss://bucket/app.jar",
                "arg1", "arg2"});
        assertEquals("com.example.Main", a.getClassName());
        assertEquals("oss://bucket/app.jar", a.getResource());
        assertEquals("SPARK", a.getBatchType());
        assertEquals(2, a.getArgs().size());
        assertEquals("arg1", a.getArgs().get(0));
    }

    @Test
    public void shortClassFlag_isEquivalent() {
        SparkSubmitArgs a = SparkSubmitParser.parse(new String[] {"-c", "com.example.Main", "x.jar"});
        assertEquals("com.example.Main", a.getClassName());
    }

    @Test
    public void detectsEachResourceForm() {
        assertEquals("a.jar", SparkSubmitParser.parse(new String[] {"a.jar"}).getResource());
        assertEquals("local:///opt/app.jar",
                SparkSubmitParser.parse(new String[] {"local:///opt/app.jar"}).getResource());
        assertEquals("oss://b/app.jar",
                SparkSubmitParser.parse(new String[] {"oss://b/app.jar"}).getResource());
        assertEquals("oss://b/main.py",
                SparkSubmitParser.parse(new String[] {"oss://b/main.py"}).getResource());
    }

    @Test
    public void firstNonResourcePositional_becomesAppArg() {
        // a bare token that is not a recognised resource is treated as an application argument
        SparkSubmitArgs a = SparkSubmitParser.parse(new String[] {"justanarg"});
        assertNull(a.getResource());
        assertEquals(1, a.getArgs().size());
        assertEquals("justanarg", a.getArgs().get(0));
    }

    @Test
    public void secondJarAfterResource_becomesAppArg() {
        // once the resource is set, further jar-looking tokens are application arguments
        SparkSubmitArgs a = SparkSubmitParser.parse(new String[] {"app.jar", "other.jar"});
        assertEquals("app.jar", a.getResource());
        assertEquals(1, a.getArgs().size());
        assertEquals("other.jar", a.getArgs().get(0));
    }

    @Test
    public void infersPysparkFromPyResource() {
        SparkSubmitArgs a = SparkSubmitParser.parse(new String[] {"oss://bucket/main.py"});
        assertEquals("PYSPARK", a.getBatchType());
        assertEquals("oss://bucket/main.py", a.getResource());
    }

    // ---- --conf and the memory/executor shortcuts ----

    @Test
    public void parsesConfIntoMapAndMapsShortcuts() {
        SparkSubmitArgs a = SparkSubmitParser.parse(new String[] {
                "--conf", "spark.executor.memory=2g",
                "--num-executors", "5",
                "--driver-memory", "1g",
                "--class", "X", "x.jar"});
        assertEquals("2g", a.getConf().get("spark.executor.memory"));
        assertEquals("5", a.getConf().get("spark.executor.instances"));
        assertEquals("1g", a.getConf().get("spark.driver.memory"));
    }

    @Test
    public void confWithoutEquals_isIgnored() {
        SparkSubmitArgs a = SparkSubmitParser.parse(new String[] {
                "--conf", "novalue", "--conf", "=leading", "--class", "X", "x.jar"});
        assertTrue("malformed --conf entries are dropped", a.getConf().isEmpty());
    }

    @Test
    public void confValueMayContainEquals() {
        SparkSubmitArgs a = SparkSubmitParser.parse(new String[] {
                "--conf", "spark.k=a=b=c", "--class", "X", "x.jar"});
        assertEquals("a=b=c", a.getConf().get("spark.k"));
    }

    @Test
    public void mapsExecutorAndCoresShortcuts() {
        SparkSubmitArgs a = SparkSubmitParser.parse(new String[] {
                "--executor-cores", "4",
                "--total-executor-cores", "16",
                "--driver-cores", "2",
                "--driver-class-path", "/opt/cp",
                "--driver-java-options", "-Dx=1",
                "--driver-library-path", "/opt/lib",
                "--class", "X", "x.jar"});
        assertEquals("4", a.getConf().get("spark.executor.cores"));
        assertEquals("16", a.getConf().get("spark.cores.max"));
        assertEquals("2", a.getConf().get("spark.driver.cores"));
        assertEquals("2", a.getDriverCores());
        assertEquals("/opt/cp", a.getConf().get("spark.driver.extraClassPath"));
        assertEquals("-Dx=1", a.getConf().get("spark.driver.extraJavaOptions"));
        assertEquals("/opt/lib", a.getConf().get("spark.driver.extraLibraryPath"));
    }

    // ---- comma-separated list options ----

    @Test
    public void splitsCommaSeparatedJars() {
        SparkSubmitArgs a = SparkSubmitParser.parse(new String[] {
                "--jars", "a.jar, b.jar ,c.jar", "--class", "X", "x.jar"});
        assertEquals(3, a.getJars().size());
        assertTrue(a.getJars().contains("b.jar"));
    }

    @Test
    public void splitsAllListOptionsAndTrimsEmpties() {
        SparkSubmitArgs a = SparkSubmitParser.parse(new String[] {
                "--py-files", "x.py,, y.py ",
                "--files", "a.txt,b.txt",
                "--archives", "arc.zip",
                "--packages", "g:a:1, g:b:2",
                "--repositories", "http://r1 , http://r2",
                "oss://b/main.py"});
        assertEquals(2, a.getPyFiles().size());
        assertTrue(a.getPyFiles().contains("y.py"));
        assertEquals(2, a.getFiles().size());
        assertEquals(1, a.getArchives().size());
        assertEquals(2, a.getPackages().size());
        assertTrue(a.getPackages().contains("g:b:2"));
        assertEquals(2, a.getRepositories().size());
        assertTrue(a.getRepositories().contains("http://r2"));
    }

    // ---- queue / proxy-user / deploy-mode ----

    @Test
    public void parsesQueueProxyUserAndDeployMode() {
        SparkSubmitArgs a = SparkSubmitParser.parse(new String[] {
                "--queue", "etl",
                "--proxy-user", "alice",
                "--deploy-mode", "cluster",
                "--class", "X", "x.jar"});
        assertEquals("etl", a.getQueue());
        assertEquals("etl", a.getConf().get("spark.yarn.queue"));
        assertEquals("alice", a.getProxyUser());
        assertEquals("cluster", a.getDeployMode());
    }

    // ---- name / status / kill ----

    @Test
    public void parsesName() {
        SparkSubmitArgs a = SparkSubmitParser.parse(new String[] {"--name", "myjob", "x.jar"});
        assertEquals("myjob", a.getName());
    }

    @Test
    public void parsesStatusAndKillIds() {
        assertEquals("b-1", SparkSubmitParser.parse(new String[] {"--status", "b-1"}).getStatusBatchId());
        assertEquals("b-2", SparkSubmitParser.parse(new String[] {"--kill", "b-2"}).getKillBatchId());
    }

    // ---- driver-log streaming flags (tri-state) ----

    @Test
    public void driverLogStream_isUnsetByDefault_butResolvesToOn() {
        SparkSubmitArgs a = SparkSubmitParser.parse(new String[] {"--class", "X", "x.jar"});
        assertNull("no flag -> tri-state null", a.getDriverLogStream());
        assertTrue("resolved default is on", a.isDriverLogStream());
    }

    @Test
    public void noDriverLogFlag_disablesStreaming() {
        SparkSubmitArgs a = SparkSubmitParser.parse(new String[] {
                "--no-driver-log", "--class", "X", "x.jar"});
        assertEquals(Boolean.FALSE, a.getDriverLogStream());
        assertFalse(a.isDriverLogStream());
    }

    @Test
    public void driverLogFlag_enablesStreaming() {
        SparkSubmitArgs a = SparkSubmitParser.parse(new String[] {
                "--driver-log", "--class", "X", "x.jar"});
        assertEquals(Boolean.TRUE, a.getDriverLogStream());
        assertTrue(a.isDriverLogStream());
    }

    @Test
    public void parsesDriverLogGrepFlags() {
        SparkSubmitArgs a = SparkSubmitParser.parse(new String[] {
                "--driver-log-grep", "WARN|ERROR",
                "--driver-log-grep-v", "TaskSetManager",
                "--class", "X", "x.jar"});
        assertEquals("WARN|ERROR", a.getDriverLogGrep());
        assertEquals("TaskSetManager", a.getDriverLogGrepV());
    }

    // ---- SQL / session ----

    @Test
    public void parsesSqlStatementAndSessionFlags() {
        SparkSubmitArgs a = SparkSubmitParser.parse(new String[] {"-e", "SHOW DATABASES", "--session"});
        assertEquals("SHOW DATABASES", a.getSqlStatement());
        assertTrue(a.isSqlMode());
        assertTrue(a.isSqlSessionMode());
        assertFalse(a.isSqlBatchMode());
    }

    @Test
    public void parsesSqlFile_defaultsToBatchMode() {
        SparkSubmitArgs a = SparkSubmitParser.parse(new String[] {"-f", "/tmp/q.sql"});
        assertEquals("/tmp/q.sql", a.getSqlFile());
        assertTrue(a.isSqlMode());
        assertTrue("no --session => batch mode", a.isSqlBatchMode());
        assertFalse(a.isSqlSessionMode());
    }

    // ---- timeout ----

    @Test
    public void parsesValidTimeout() {
        SparkSubmitArgs a = SparkSubmitParser.parse(new String[] {"--timeout", "300", "x.jar"});
        assertEquals(Long.valueOf(300), a.getTimeoutSeconds());
    }

    @Test
    public void invalidTimeout_isIgnoredNotFatal() {
        SparkSubmitArgs a = SparkSubmitParser.parse(new String[] {"--timeout", "abc", "x.jar"});
        assertNull("a non-numeric timeout is warned and ignored, not fatal", a.getTimeoutSeconds());
    }

    // ---- connection overrides ----

    @Test
    public void parsesConnectionOverrides() {
        SparkSubmitArgs a = SparkSubmitParser.parse(new String[] {
                "--kyuubi-url", "http://h:10099",
                "--kyuubi-user", "bob",
                "--kyuubi-password", "secret",
                "--history-url", "http://hist:18080",
                "--config-file", "/etc/my.conf",
                "--class", "X", "x.jar"});
        assertEquals("http://h:10099", a.getKyuubiUrl());
        assertEquals("bob", a.getKyuubiUser());
        assertEquals("secret", a.getKyuubiPassword());
        assertEquals("http://hist:18080", a.getSparkHistoryUrl());
        assertEquals("/etc/my.conf", a.getConfigFile());
    }

    // ---- tolerated noise: properties-file + unknown options ----

    @Test
    public void propertiesFileIsSkipped_withoutConsumingResource() {
        SparkSubmitArgs a = SparkSubmitParser.parse(new String[] {
                "--properties-file", "/etc/spark.conf", "--class", "X", "x.jar"});
        assertEquals("x.jar", a.getResource());
        assertEquals("X", a.getClassName());
    }

    @Test
    public void unknownOptionWithValue_consumesItsValue() {
        // an unknown --flag swallows the following non-option token, so it never pollutes appArgs
        SparkSubmitArgs a = SparkSubmitParser.parse(new String[] {
                "--unknown-opt", "someValue", "--class", "X", "x.jar"});
        assertEquals("x.jar", a.getResource());
        assertTrue("the unknown option's value is not an app arg", a.getArgs().isEmpty());
    }

    @Test
    public void unknownOptionBeforeAnotherOption_takesNoValue() {
        SparkSubmitArgs a = SparkSubmitParser.parse(new String[] {
                "--unknown-flag", "--name", "n", "x.jar"});
        assertEquals("the following option is still parsed", "n", a.getName());
        assertEquals("x.jar", a.getResource());
    }

    @Test
    public void unknownOptionAtEnd_isHarmless() {
        SparkSubmitArgs a = SparkSubmitParser.parse(new String[] {"x.jar", "--unknown-trailing"});
        assertEquals("x.jar", a.getResource());
    }

    // ---- missing trailing values must not crash ----

    @Test
    public void trailingFlagWithoutValue_isHarmless() {
        SparkSubmitArgs a = SparkSubmitParser.parse(new String[] {"--name"});
        assertNull(a.getName());
    }
}
