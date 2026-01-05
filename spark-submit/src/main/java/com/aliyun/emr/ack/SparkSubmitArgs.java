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
}

