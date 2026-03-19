package com.aliyun.emr.ack;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Parsed Spark submit arguments
 */
public class SparkSubmitArgs {
    private String name;
    private String className;
    private String resource;
    private String batchType = "SPARK";
    private String proxyUser;
    private String queue;
    private String statusBatchId;
    private String killBatchId;
    private List<String> args = new ArrayList<>();
    private Map<String, String> conf = new HashMap<>();
    private List<String> pyFiles = new ArrayList<>();
    private List<String> files = new ArrayList<>();
    private List<String> jars = new ArrayList<>();
    private List<String> archives = new ArrayList<>();
    private List<String> packages = new ArrayList<>();
    private List<String> repositories = new ArrayList<>();
    private String driverCores;
    private String deployMode;
    private String sqlFile;        // -f: SQL file path
    private String sqlStatement;   // -e: SQL statement string
    private Long timeoutSeconds;   // --timeout: job timeout in seconds
    private boolean sqlSessionMode;  // --session: use session mode for SQL (-e/-f) instead of default batch mode
    
    // Kyuubi connection configuration (command-line override)
    private String kyuubiUrl;      // --kyuubi-url: Kyuubi server URL
    private String kyuubiUser;     // --kyuubi-user: Kyuubi username
    private String kyuubiPassword; // --kyuubi-password: Kyuubi password
    private String sparkHistoryUrl; // --history-url: Spark History Server URL
    private String configFile;     // --config-file: custom config file path
    
    public String getName() {
        return name;
    }
    
    public void setName(String name) {
        this.name = name;
    }
    
    public String getClassName() {
        return className;
    }
    
    public void setClassName(String className) {
        this.className = className;
    }
    
    public String getResource() {
        return resource;
    }
    
    public void setResource(String resource) {
        this.resource = resource;
    }
    
    public List<String> getArgs() {
        return args;
    }
    
    public void setArgs(List<String> args) {
        this.args = args;
    }
    
    public Map<String, String> getConf() {
        return conf;
    }
    
    public void setConf(Map<String, String> conf) {
        this.conf = conf;
    }

    public List<String> getPyFiles() {
        return pyFiles;
    }

    public void setPyFiles(List<String> pyFiles) {
        this.pyFiles = pyFiles;
    }

    public List<String> getFiles() {
        return files;
    }

    public void setFiles(List<String> files) {
        this.files = files;
    }

    public List<String> getJars() {
        return jars;
    }

    public void setJars(List<String> jars) {
        this.jars = jars;
    }

    public List<String> getArchives() {
        return archives;
    }

    public void setArchives(List<String> archives) {
        this.archives = archives;
    }

    public List<String> getPackages() {
        return packages;
    }

    public void setPackages(List<String> packages) {
        this.packages = packages;
    }

    public List<String> getRepositories() {
        return repositories;
    }

    public void setRepositories(List<String> repositories) {
        this.repositories = repositories;
    }

    public String getBatchType() {
        return batchType;
    }

    public void setBatchType(String batchType) {
        this.batchType = batchType;
    }

    public String getProxyUser() {
        return proxyUser;
    }

    public void setProxyUser(String proxyUser) {
        this.proxyUser = proxyUser;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public String getDriverCores() {
        return driverCores;
    }

    public void setDriverCores(String driverCores) {
        this.driverCores = driverCores;
    }

    public String getStatusBatchId() {
        return statusBatchId;
    }

    public void setStatusBatchId(String statusBatchId) {
        this.statusBatchId = statusBatchId;
    }

    public String getKillBatchId() {
        return killBatchId;
    }

    public void setKillBatchId(String killBatchId) {
        this.killBatchId = killBatchId;
    }

    public String getDeployMode() {
        return deployMode;
    }

    public void setDeployMode(String deployMode) {
        this.deployMode = deployMode;
    }

    public String getSqlFile() {
        return sqlFile;
    }

    public void setSqlFile(String sqlFile) {
        this.sqlFile = sqlFile;
    }

    public String getSqlStatement() {
        return sqlStatement;
    }

    public void setSqlStatement(String sqlStatement) {
        this.sqlStatement = sqlStatement;
    }

    public Long getTimeoutSeconds() {
        return timeoutSeconds;
    }

    public void setTimeoutSeconds(Long timeoutSeconds) {
        this.timeoutSeconds = timeoutSeconds;
    }

    public boolean isSqlBatchMode() {
        return !sqlSessionMode;
    }

    public boolean isSqlSessionMode() {
        return sqlSessionMode;
    }

    public void setSqlSessionMode(boolean sqlSessionMode) {
        this.sqlSessionMode = sqlSessionMode;
    }

    /**
     * Check if this is a SQL submission mode (-f or -e)
     */
    public boolean isSqlMode() {
        return sqlFile != null || sqlStatement != null;
    }

    public String getKyuubiUrl() {
        return kyuubiUrl;
    }

    public void setKyuubiUrl(String kyuubiUrl) {
        this.kyuubiUrl = kyuubiUrl;
    }

    public String getKyuubiUser() {
        return kyuubiUser;
    }

    public void setKyuubiUser(String kyuubiUser) {
        this.kyuubiUser = kyuubiUser;
    }

    public String getKyuubiPassword() {
        return kyuubiPassword;
    }

    public void setKyuubiPassword(String kyuubiPassword) {
        this.kyuubiPassword = kyuubiPassword;
    }

    public String getSparkHistoryUrl() {
        return sparkHistoryUrl;
    }

    public void setSparkHistoryUrl(String sparkHistoryUrl) {
        this.sparkHistoryUrl = sparkHistoryUrl;
    }

    public String getConfigFile() {
        return configFile;
    }

    public void setConfigFile(String configFile) {
        this.configFile = configFile;
    }
}

