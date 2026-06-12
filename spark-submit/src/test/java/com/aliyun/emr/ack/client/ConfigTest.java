package com.aliyun.emr.ack.client;

import com.aliyun.emr.ack.cli.SparkSubmitArgs;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class ConfigTest {

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    /** System properties win over the config file, so they must be clear for file-based assertions. */
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
        File f = writeConf("kyuubi.server.url=http://host:10099\n"
                + "kyuubi.server.username=alice\n");
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
        File f = writeConf("spark.submit.driver.log.grep-v=TaskSetManager|BlockManagerInfo\n"
                + "spark.submit.driver.log.enabled=false\n");
        Config c = new Config(f.getAbsolutePath());
        assertEquals("TaskSetManager|BlockManagerInfo",
                c.getProperty("spark.submit.driver.log.grep-v"));
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
}
