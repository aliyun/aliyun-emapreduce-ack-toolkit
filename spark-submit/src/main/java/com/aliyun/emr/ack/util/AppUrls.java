package com.aliyun.emr.ack.util;

/**
 * Builds the user-facing application URLs surfaced during submission: the Spark History Server link
 * for a finished job and the live Spark Web UI link for a running one. Cluster-manager agnostic.
 */
public final class AppUrls {
    private AppUrls() {
    }

    /**
     * Spark History Server URL for an application, or null when the history server is not configured
     * or the appId is unknown. Works for both {@code application_*} (YARN) and {@code spark-*} (K8s).
     */
    public static String applicationUrl(String historyServerUrl, String appId) {
        if (historyServerUrl == null || historyServerUrl.isEmpty()
                || appId == null || appId.isEmpty()) {
            return null;
        }
        String baseUrl = historyServerUrl.trim();
        if (baseUrl.endsWith("/")) {
            baseUrl = baseUrl.substring(0, baseUrl.length() - 1);
        }
        return baseUrl + "/history/" + appId + "/1/";
    }

    /**
     * A clickable, live Spark Web UI URL for a running batch from its appUrl, or null while appUrl is
     * not yet available.
     * <ul>
     *   <li>K8s: appUrl is the driver Service DNS ({@code http://...svc[.cluster.local]:port}), only
     *       reachable inside the cluster, so route it through Kyuubi's {@code /engine-ui/} proxy.
     *       Matched on {@code .svc} rather than the UI port, which is configurable.</li>
     *   <li>YARN: appUrl is the RM tracking URL (already reachable) — returned as-is.</li>
     * </ul>
     * Only valid while the driver is alive; use the History Server for finished jobs.
     */
    public static String sparkUiUrl(String kyuubiServerUrl, String appUrl) {
        if (appUrl == null || appUrl.isEmpty()) {
            return null;
        }
        if (kyuubiServerUrl != null && !kyuubiServerUrl.isEmpty()
                && appUrl.startsWith("http://") && appUrl.contains(".svc")) {
            String base = kyuubiServerUrl.trim();
            if (base.endsWith("/")) {
                base = base.substring(0, base.length() - 1);
            }
            return base + "/engine-ui/" + appUrl.substring("http://".length()) + "/";
        }
        return appUrl;
    }
}
