package com.aliyun.emr.ack;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Configuration manager for Kyuubi server connection
 */
public class Config {
    private static final String DEFAULT_CONFIG_FILE = System.getProperty("user.home") + "/.spark-submit.conf";
    private static final String DEFAULT_SERVER_URL = "http://localhost:10099";
    private static final String DEFAULT_USERNAME = "kyuubi-server";
    private static final String DEFAULT_PASSWORD = "kyuubi-server";
    
    private String serverUrl;
    private String username;
    private String password;
    private String sparkHistoryServerUrl;
    private boolean usingDefaultConfig;
    private String configFile;
    
    public Config() {
        loadConfig();
    }
    
    public Config(String configFile) {
        loadConfig(configFile);
    }
    
    private void loadConfig() {
        loadConfig(DEFAULT_CONFIG_FILE);
    }
    
    private void loadConfig(String configFile) {
        this.configFile = configFile;
        Properties props = new Properties();
        File file = new File(configFile);
        boolean configFileExists = file.exists();
        
        if (configFileExists) {
            try (FileInputStream fis = new FileInputStream(file)) {
                props.load(fis);
            } catch (IOException e) {
                System.err.println("Warning: Failed to load config file: " + configFile + ", error: " + e.getMessage());
                e.printStackTrace();
            }
        } else {
            System.err.println("Info: Config file not found: " + configFile);
        }
        
        // Load from system properties or environment variables with fallback to config file
        String envUrl = System.getenv("KYUUBI_SERVER_URL");
        String propUrl = System.getProperty("kyuubi.server.url");
        String fileUrl = props.getProperty("kyuubi.server.url");
        
        // Always print debug info to help diagnose configuration issues
        System.err.println("Config loading info:");
        System.err.println("  Config file path: " + configFile);
        System.err.println("  Config file exists: " + configFileExists);
        if (configFileExists) {
            System.err.println("  Config file properties loaded: " + props.size() + " properties");
            props.stringPropertyNames().forEach(key -> 
                System.err.println("    " + key + " = " + props.getProperty(key))
            );
        }
        System.err.println("  Environment KYUUBI_SERVER_URL: " + (envUrl != null ? envUrl : "(not set)"));
        System.err.println("  System property kyuubi.server.url: " + (propUrl != null ? propUrl : "(not set)"));
        System.err.println("  Config file kyuubi.server.url: " + (fileUrl != null ? fileUrl : "(not set)"));
        
        this.serverUrl = propUrl != null ? propUrl : 
            (envUrl != null ? envUrl : (fileUrl != null ? fileUrl : DEFAULT_SERVER_URL));
        
        System.err.println("  Final server URL: " + this.serverUrl);
        
        String envUsername = System.getenv("KYUUBI_SERVER_USERNAME");
        String propUsername = System.getProperty("kyuubi.server.username");
        String fileUsername = props.getProperty("kyuubi.server.username");
        
        this.username = propUsername != null ? propUsername :
            (envUsername != null ? envUsername : (fileUsername != null ? fileUsername : DEFAULT_USERNAME));
        
        String envPassword = System.getenv("KYUUBI_SERVER_PASSWORD");
        String propPassword = System.getProperty("kyuubi.server.password");
        String filePassword = props.getProperty("kyuubi.server.password");
        
        this.password = propPassword != null ? propPassword :
            (envPassword != null ? envPassword : (filePassword != null ? filePassword : DEFAULT_PASSWORD));
        
        // Load Spark History Server URL (optional)
        String envHistoryUrl = System.getenv("SPARK_HISTORY_SERVER_URL");
        String propHistoryUrl = System.getProperty("spark.history.server.url");
        String fileHistoryUrl = props.getProperty("spark.history.server.url");
        
        this.sparkHistoryServerUrl = propHistoryUrl != null ? propHistoryUrl :
            (envHistoryUrl != null ? envHistoryUrl : fileHistoryUrl);
        
        // Check if using default values
        this.usingDefaultConfig = !configFileExists && 
            (envUrl == null && propUrl == null) &&
            (envUsername == null && propUsername == null) &&
            (envPassword == null && propPassword == null);
    }
    
    public String getServerUrl() {
        return serverUrl;
    }
    
    public String getUsername() {
        return username;
    }
    
    public String getPassword() {
        return password;
    }
    
    public boolean isUsingDefaultConfig() {
        return usingDefaultConfig;
    }
    
    public String getConfigFile() {
        return configFile;
    }
    
    public String getSparkHistoryServerUrl() {
        return sparkHistoryServerUrl;
    }
    
    public String getBaseUrl() {
        String url = serverUrl;
        if (!url.endsWith("/")) {
            url += "/";
        }
        if (!url.contains("/api/v1")) {
            url += "api/v1";
        }
        return url;
    }
    
    public void validateAndPrintWarning() {
        if (usingDefaultConfig) {
            System.err.println("\n⚠️  Warning: Using default configuration!");
            System.err.println("   Kyuubi Server URL: " + serverUrl);
            System.err.println("   Username: " + username);
            System.err.println("\n   To configure Kyuubi server, please:");
            System.err.println("   1. Create config file: " + configFile);
            System.err.println("   2. Add the following content:");
            System.err.println("      kyuubi.server.url=<your-kyuubi-server-url>");
            System.err.println("      kyuubi.server.username=<your-username>");
            System.err.println("      kyuubi.server.password=<your-password>");
            System.err.println("      spark.history.server.url=<your-spark-history-server-url>  # Optional");
            System.err.println("\n   Or set environment variables:");
            System.err.println("      export KYUUBI_SERVER_URL=<your-kyuubi-server-url>");
            System.err.println("      export KYUUBI_SERVER_USERNAME=<your-username>");
            System.err.println("      export KYUUBI_SERVER_PASSWORD=<your-password>");
            System.err.println("      export SPARK_HISTORY_SERVER_URL=<your-spark-history-server-url>  # Optional");
            System.err.println("\n   Or use system properties:");
            System.err.println("      -Dkyuubi.server.url=<your-kyuubi-server-url>");
            System.err.println("      -Dkyuubi.server.username=<your-username>");
            System.err.println("      -Dkyuubi.server.password=<your-password>");
            System.err.println("      -Dspark.history.server.url=<your-spark-history-server-url>  # Optional");
            System.err.println();
        } else {
            // Print loaded configuration for verification
            System.err.println("Info: Loaded configuration from: " + configFile);
            System.err.println("Info: Kyuubi Server URL: " + serverUrl);
            System.err.println("Info: Username: " + username);
        }
    }
}

