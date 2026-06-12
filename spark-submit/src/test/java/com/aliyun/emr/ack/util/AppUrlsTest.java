package com.aliyun.emr.ack.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

public class AppUrlsTest {

    @Test
    public void applicationUrl_returnsNullWhenHistoryServerOrAppIdIsMissing() {
        assertNull(AppUrls.applicationUrl(null, "app-1"));
        assertNull(AppUrls.applicationUrl("", "app-1"));
        assertNull(AppUrls.applicationUrl("http://history", null));
        assertNull(AppUrls.applicationUrl("http://history", ""));
    }

    @Test
    public void applicationUrl_normalizesTrailingSlash() {
        assertEquals(
                "http://history/history/application_1/1/",
                AppUrls.applicationUrl("http://history/", "application_1"));
        assertEquals(
                "http://history/history/spark-1/1/",
                AppUrls.applicationUrl("http://history", "spark-1"));
    }

    @Test
    public void sparkUiUrl_routesKubernetesServiceThroughKyuubiProxy() {
        assertEquals(
                "http://kyuubi:10099/engine-ui/driver-ui.kyuubi.svc.cluster.local:4040/",
                AppUrls.sparkUiUrl(
                        "http://kyuubi:10099/", "http://driver-ui.kyuubi.svc.cluster.local:4040"));
    }

    @Test
    public void sparkUiUrl_returnsNonKubernetesUrlAsIs() {
        assertEquals(
                "http://rm:8088/proxy/application_1/",
                AppUrls.sparkUiUrl("http://kyuubi:10099", "http://rm:8088/proxy/application_1/"));
    }

    @Test
    public void sparkUiUrl_returnsNullForMissingAppUrl() {
        assertNull(AppUrls.sparkUiUrl("http://kyuubi:10099", null));
        assertNull(AppUrls.sparkUiUrl("http://kyuubi:10099", ""));
    }
}
