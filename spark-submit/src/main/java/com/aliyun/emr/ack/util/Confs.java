package com.aliyun.emr.ack.util;

import com.aliyun.emr.ack.client.Config;
import java.util.Map;

/**
 * Resolve a config value from the per-submit {@code --conf} map, falling back to the loaded Config.
 */
public final class Confs {
    private Confs() {}

    public static String value(String key, Map<String, String> conf, Config config) {
        String v = conf.get(key);
        return v != null ? v : config.getProperty(key);
    }
}
