package com.aliyun.emr.ack;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Parser for spark-submit command line arguments
 */
public class SparkSubmitParser {
    
    public static SparkSubmitArgs parse(String[] args) {
        SparkSubmitArgs result = new SparkSubmitArgs();
        List<String> appArgs = new ArrayList<>();
        Map<String, String> conf = new HashMap<>();
        List<String> pyFiles = new ArrayList<>();
        List<String> files = new ArrayList<>();
        List<String> jars = new ArrayList<>();
        List<String> archives = new ArrayList<>();
        List<String> packages = new ArrayList<>();
        List<String> repositories = new ArrayList<>();
        
        int i = 0;
        while (i < args.length) {
            String arg = args[i];
            
            if ("--name".equals(arg)) {
                if (i + 1 < args.length) {
                    result.setName(args[++i]);
                }
            } else if ("--status".equals(arg)) {
                if (i + 1 < args.length) {
                    result.setStatusBatchId(args[++i]);
                }
            } else if ("--kill".equals(arg)) {
                if (i + 1 < args.length) {
                    result.setKillBatchId(args[++i]);
                }
            } else if ("--class".equals(arg) || "-c".equals(arg)) {
                if (i + 1 < args.length) {
                    result.setClassName(args[++i]);
                }
            } else if ("--conf".equals(arg)) {
                if (i + 1 < args.length) {
                    String confValue = args[++i];
                    int eqIndex = confValue.indexOf('=');
                    if (eqIndex > 0) {
                        String key = confValue.substring(0, eqIndex);
                        String value = confValue.substring(eqIndex + 1);
                        conf.put(key, value);
                    }
                }
            } else if ("--properties-file".equals(arg)) {
                // Skip properties file for now
                if (i + 1 < args.length) {
                    i++;
                }
            } else if ("--jars".equals(arg)) {
                if (i + 1 < args.length) {
                    String value = args[++i];
                    for (String jar : value.split(",")) {
                        String trimmed = jar.trim();
                        if (!trimmed.isEmpty()) {
                            jars.add(trimmed);
                        }
                    }
                }
            } else if ("--packages".equals(arg)) {
                if (i + 1 < args.length) {
                    String value = args[++i];
                    for (String p : value.split(",")) {
                        String trimmed = p.trim();
                        if (!trimmed.isEmpty()) {
                            packages.add(trimmed);
                        }
                    }
                }
            } else if ("--repositories".equals(arg)) {
                if (i + 1 < args.length) {
                    String value = args[++i];
                    for (String repo : value.split(",")) {
                        String trimmed = repo.trim();
                        if (!trimmed.isEmpty()) {
                            repositories.add(trimmed);
                        }
                    }
                }
            } else if ("--py-files".equals(arg)) {
                if (i + 1 < args.length) {
                    String value = args[++i];
                    for (String py : value.split(",")) {
                        String trimmed = py.trim();
                        if (!trimmed.isEmpty()) {
                            pyFiles.add(trimmed);
                        }
                    }
                }
            } else if ("--files".equals(arg)) {
                if (i + 1 < args.length) {
                    String value = args[++i];
                    for (String file : value.split(",")) {
                        String trimmed = file.trim();
                        if (!trimmed.isEmpty()) {
                            files.add(trimmed);
                        }
                    }
                }
            } else if ("--archives".equals(arg)) {
                if (i + 1 < args.length) {
                    String value = args[++i];
                    for (String archive : value.split(",")) {
                        String trimmed = archive.trim();
                        if (!trimmed.isEmpty()) {
                            archives.add(trimmed);
                        }
                    }
                }
            } else if ("--driver-memory".equals(arg)) {
                if (i + 1 < args.length) {
                    conf.put("spark.driver.memory", args[++i]);
                }
            } else if ("--driver-cores".equals(arg)) {
                if (i + 1 < args.length) {
                    String value = args[++i];
                    result.setDriverCores(value);
                    conf.put("spark.driver.cores", value);
                }
            } else if ("--driver-class-path".equals(arg)) {
                if (i + 1 < args.length) {
                    conf.put("spark.driver.extraClassPath", args[++i]);
                }
            } else if ("--driver-java-options".equals(arg)) {
                if (i + 1 < args.length) {
                    conf.put("spark.driver.extraJavaOptions", args[++i]);
                }
            } else if ("--driver-library-path".equals(arg)) {
                if (i + 1 < args.length) {
                    conf.put("spark.driver.extraLibraryPath", args[++i]);
                }
            } else if ("--executor-memory".equals(arg)) {
                if (i + 1 < args.length) {
                    conf.put("spark.executor.memory", args[++i]);
                }
            } else if ("--executor-cores".equals(arg)) {
                if (i + 1 < args.length) {
                    conf.put("spark.executor.cores", args[++i]);
                }
            } else if ("--num-executors".equals(arg)) {
                if (i + 1 < args.length) {
                    conf.put("spark.executor.instances", args[++i]);
                }
            } else if ("--total-executor-cores".equals(arg)) {
                if (i + 1 < args.length) {
                    conf.put("spark.cores.max", args[++i]);
                }
            } else if ("--queue".equals(arg)) {
                if (i + 1 < args.length) {
                    String queue = args[++i];
                    result.setQueue(queue);
                    conf.put("spark.yarn.queue", queue);
                }
            } else if ("--proxy-user".equals(arg)) {
                if (i + 1 < args.length) {
                    result.setProxyUser(args[++i]);
                }
            } else if ("--deploy-mode".equals(arg)) {
                if (i + 1 < args.length) {
                    result.setDeployMode(args[++i]);
                }
            } else if (arg.startsWith("--")) {
                // Unknown option, skip
                if (i + 1 < args.length && !args[i + 1].startsWith("-")) {
                    i++;
                }
            } else {
                // This should be the resource (jar file) or application arguments
                if (result.getResource() == null && (arg.endsWith(".jar") || arg.endsWith(".py") ||
                    arg.startsWith("local://") || arg.startsWith("oss://"))) {
                    result.setResource(arg);
                } else {
                    appArgs.add(arg);
                }
            }
            i++;
        }
        
        result.setConf(conf);
        result.setArgs(appArgs);
        result.setPyFiles(pyFiles);
        result.setFiles(files);
        result.setJars(jars);
        result.setArchives(archives);
        result.setPackages(packages);
        result.setRepositories(repositories);

        // Infer batch type
        if (result.getResource() != null && result.getResource().endsWith(".py")) {
            result.setBatchType("PYSPARK");
        } else {
            result.setBatchType("SPARK");
        }
        
        return result;
    }
}

