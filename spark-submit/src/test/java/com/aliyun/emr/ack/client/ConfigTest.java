package com.aliyun.emr.ack.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import com.aliyun.emr.ack.cli.SparkSubmitArgs;
import com.aliyun.emr.ack.command.DriverLogStreamer;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ConfigTest {

    @Rule public TemporaryFolder tmp = new TemporaryFolder();

    /**
     * System properties win over the config file, so they must be clear for file-based assertions.
     */
    private static boolean serverEnvOrPropSet() {
        return System.getProperty("kyuubi.server.url") != null
                || System.getenv("KYUUBI_SERVER_URL") != null;
    }

    private File writeConf(String content) throws IOException {
        File f = tmp.newFile("test.conf");
        try (FileWriter w = new FileWriter(f)) {
            w.write(content);
        }
        return f;
    }

    @Test
    public void readsServerUrlFromConfigFile() throws IOException {
        assumeTrue("env/sysprop override present", !serverEnvOrPropSet());
        File f =
                writeConf(
                        "kyuubi.server.url=http://host:10099\n" + "kyuubi.server.username=alice\n");
        Config c = new Config(f.getAbsolutePath());
        assertEquals("http://host:10099", c.getServerUrl());
        assertEquals("alice", c.getUsername());
        assertFalse(c.isUsingDefaultConfig());
    }

    @Test
    public void missingFile_usesDefaultsAndFlagsDefaultConfig() {
        assumeTrue("env/sysprop override present", !serverEnvOrPropSet());
        Config c = new Config(tmp.getRoot().getAbsolutePath() + "/does-not-exist.conf");
        assertEquals("http://localhost:10099", c.getServerUrl());
        assertTrue(c.isUsingDefaultConfig());
    }

    @Test
    public void getProperty_exposesArbitraryConfigKeys() throws IOException {
        File f =
                writeConf(
                        "spark.submit.driver.log.grep-v=TaskSetManager|BlockManagerInfo\n"
                                + "spark.submit.driver.log.enabled=false\n");
        Config c = new Config(f.getAbsolutePath());
        assertEquals(
                "TaskSetManager|BlockManagerInfo", c.getProperty("spark.submit.driver.log.grep-v"));
        assertEquals("false", c.getProperty("spark.submit.driver.log.enabled"));
        assertNull(c.getProperty("spark.submit.driver.log.grep"));
    }

    @Test
    public void cliOverrideTakesPrecedenceOverFile() throws IOException {
        assumeTrue("env/sysprop override present", !serverEnvOrPropSet());
        File f = writeConf("kyuubi.server.url=http://from-file:10099\n");
        Config c = new Config(f.getAbsolutePath());
        SparkSubmitArgs args = new SparkSubmitArgs();
        args.setKyuubiUrl("http://from-cli:10099");
        args.setKyuubiUser("cliuser");
        c.applyOverrides(args);
        assertEquals("http://from-cli:10099", c.getServerUrl());
        assertEquals("cliuser", c.getUsername());
    }

    @Test
    public void systemPropertiesTakePrecedenceOverConfigFile() throws IOException {
        String oldUrl = System.getProperty("kyuubi.server.url");
        String oldUser = System.getProperty("kyuubi.server.username");
        String oldPassword = System.getProperty("kyuubi.server.password");
        String oldHistory = System.getProperty("spark.history.server.url");
        try {
            System.setProperty("kyuubi.server.url", "http://from-system:10099");
            System.setProperty("kyuubi.server.username", "system-user");
            System.setProperty("kyuubi.server.password", "system-password");
            System.setProperty("spark.history.server.url", "http://history-system:18080");

            File f =
                    writeConf(
                            "kyuubi.server.url=http://from-file:10099\n"
                                    + "kyuubi.server.username=file-user\n"
                                    + "kyuubi.server.password=file-password\n"
                                    + "spark.history.server.url=http://history-file:18080\n");
            Config c = new Config(f.getAbsolutePath());

            assertEquals("http://from-system:10099", c.getServerUrl());
            assertEquals("system-user", c.getUsername());
            assertEquals("system-password", c.getPassword());
            assertEquals("http://history-system:18080", c.getSparkHistoryServerUrl());
            assertFalse(c.isUsingDefaultConfig());
        } finally {
            restoreProperty("kyuubi.server.url", oldUrl);
            restoreProperty("kyuubi.server.username", oldUser);
            restoreProperty("kyuubi.server.password", oldPassword);
            restoreProperty("spark.history.server.url", oldHistory);
        }
    }

    @Test
    public void getBaseUrlNormalizesTrailingSlashAndApiPrefix() throws IOException {
        SparkSubmitArgs args = new SparkSubmitArgs();
        args.setKyuubiUrl("http://host:10099");
        Config c = new Config(tmp.getRoot().getAbsolutePath() + "/missing-base.conf");
        c.applyOverrides(args);
        assertEquals("http://host:10099/api/v1", c.getBaseUrl());

        args.setKyuubiUrl("http://host:10099/api/v1");
        c = new Config(tmp.getRoot().getAbsolutePath() + "/missing-base-2.conf");
        c.applyOverrides(args);
        assertEquals("http://host:10099/api/v1", c.getBaseUrl());
    }

    @Test
    public void driverLogDefaultsAreAppliedFromConfigOnlyWhenCliDidNotSetThem() throws IOException {
        File f =
                writeConf(
                        "spark.submit.driver.log.enabled=false\n"
                                + "spark.submit.driver.log.grep=ERROR\n"
                                + "spark.submit.driver.log.grep-v=TaskSetManager\n");
        Config c = new Config(f.getAbsolutePath());

        SparkSubmitArgs defaults = new SparkSubmitArgs();
        DriverLogStreamer.applyConfigDefaults(defaults, c);
        assertEquals(Boolean.FALSE, defaults.getDriverLogStream());
        assertEquals("ERROR", defaults.getDriverLogGrep());
        assertEquals("TaskSetManager", defaults.getDriverLogGrepV());

        SparkSubmitArgs cliWins = new SparkSubmitArgs();
        cliWins.setDriverLogStream(true);
        cliWins.setDriverLogGrep("WARN");
        cliWins.setDriverLogGrepV("BlockManager");
        DriverLogStreamer.applyConfigDefaults(cliWins, c);
        assertEquals(Boolean.TRUE, cliWins.getDriverLogStream());
        assertEquals("WARN", cliWins.getDriverLogGrep());
        assertEquals("BlockManager", cliWins.getDriverLogGrepV());
    }

    private static void restoreProperty(String key, String value) {
        if (value == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, value);
        }
    }
}
