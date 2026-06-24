package com.aliyun.emr.ack.command;

import com.aliyun.emr.ack.cli.*;
import com.aliyun.emr.ack.client.*;
import com.aliyun.emr.ack.util.*;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClients;

/**
 * Stages large SQL out of the K8s pod spec. SQL over {@link #THRESHOLD_BYTES} is uploaded and
 * passed as {@code -f <url>} instead of inline {@code -e}, via a dual strategy:
 *
 * <ol>
 *   <li>Kyuubi server-side upload (needs the kyuubi-upload-plugin; no client-side OSS config); then
 *   <li>client-side OSS upload (needs OSS credentials in {@code --conf}) as a fallback.
 * </ol>
 */
final class SqlUploader {
    private SqlUploader() {}

    /**
     * SQL larger than this (bytes) is uploaded rather than inlined; conservative for CJK + pod
     * overhead.
     */
    static final int THRESHOLD_BYTES = 10 * 1024;

    static String upload(
            KyuubiClient client,
            byte[] sqlBytes,
            Map<String, String> conf,
            Config config,
            Retry.RetryConfig uploadRetryCfg)
            throws IOException {
        System.err.println(
                "["
                        + Console.timestamp()
                        + "] SQL content is "
                        + (sqlBytes.length / 1024)
                        + " KB (threshold: "
                        + (THRESHOLD_BYTES / 1024)
                        + " KB), uploading...");

        // Strategy 1: Kyuubi server-side upload (zero client-side OSS config needed)
        try {
            String uri =
                    Retry.execute(
                            "kyuubiUploadFile",
                            uploadRetryCfg,
                            () -> client.uploadFile(sqlBytes, "query.sql"));
            System.err.println(
                    "[" + Console.timestamp() + "] SQL uploaded via Kyuubi server: " + uri);
            return uri;
        } catch (IOException e) {
            String msg = e.getMessage();
            // 404/405 means the kyuubi-upload-plugin is not installed → fall back to OSS.
            // Prefer the typed status code; keep the string check as a safety net.
            boolean pluginNotAvailable =
                    (e instanceof HttpStatusException
                                    && (((HttpStatusException) e).getStatusCode() == 404
                                            || ((HttpStatusException) e).getStatusCode() == 405))
                            || (msg != null
                                    && (msg.contains("HTTP 404")
                                            || msg.contains("HTTP 405")
                                            || msg.contains("Not Found")));
            if (pluginNotAvailable) {
                System.err.println(
                        "["
                                + Console.timestamp()
                                + "] Kyuubi upload plugin not available, "
                                + "trying client-side OSS upload...");
            } else {
                throw e;
            }
        }

        // Strategy 2: Client-side OSS upload (fallback for older Kyuubi without the plugin)
        return uploadToOss(sqlBytes, conf, config, uploadRetryCfg);
    }

    private static String uploadToOss(
            byte[] sqlBytes,
            Map<String, String> conf,
            Config config,
            Retry.RetryConfig uploadRetryCfg)
            throws IOException {
        String accessKeyId = Confs.value("spark.hadoop.fs.oss.accessKeyId", conf, config);
        String accessKeySecret = Confs.value("spark.hadoop.fs.oss.accessKeySecret", conf, config);
        String endpoint = Confs.value("spark.hadoop.fs.oss.endpoint", conf, config);
        String uploadPath = Confs.value("spark.kubernetes.file.upload.path", conf, config);

        if (accessKeyId == null
                || accessKeySecret == null
                || endpoint == null
                || uploadPath == null) {
            StringBuilder missing = new StringBuilder();
            if (accessKeyId == null)
                missing.append("\n  spark.hadoop.fs.oss.accessKeyId=<your-access-key-id>");
            if (accessKeySecret == null)
                missing.append("\n  spark.hadoop.fs.oss.accessKeySecret=<your-access-key-secret>");
            if (endpoint == null) missing.append("\n  spark.hadoop.fs.oss.endpoint=<oss-endpoint>");
            if (uploadPath == null)
                missing.append(
                        "\n  spark.kubernetes.file.upload.path=oss://<bucket>/<staging-path>");
            throw new IOException(
                    "SQL content is "
                            + (sqlBytes.length / 1024)
                            + " KB, exceeds "
                            + (THRESHOLD_BYTES / 1024)
                            + " KB threshold. "
                            + "OSS upload is required but the following configurations are missing:"
                            + missing
                            + "\n\nAdd via --conf or in ~/.spark-submit.conf");
        }

        String[] parsed = OssUploader.parseOssPath(uploadPath);
        if (parsed == null) {
            throw new IOException(
                    "Invalid spark.kubernetes.file.upload.path: "
                            + uploadPath
                            + ". Expected format: oss://<bucket>/<path>");
        }

        String bucket = parsed[0];
        String basePath = parsed[1];
        // Generated once, OUTSIDE the retry loop, so retries overwrite the same key
        // (PUT is idempotent — no orphan objects). Each retry recomputes Date/MD5/signature
        // internally.
        String objectKey = basePath + "/spark-sql-upload/" + UUID.randomUUID().toString() + ".sql";
        String publicEndpoint = OssUploader.toPublicEndpoint(endpoint);

        System.out.println(
                "["
                        + Console.timestamp()
                        + "] SQL content is "
                        + (sqlBytes.length / 1024)
                        + " KB, uploading to OSS...");

        // Disable HttpClient's built-in retries so application-level Retry is the single source.
        CloseableHttpClient httpClient =
                HttpClients.custom()
                        .setRetryHandler(new DefaultHttpRequestRetryHandler(0, false))
                        .build();
        try {
            String ossUrl =
                    Retry.execute(
                            "ossUpload",
                            uploadRetryCfg,
                            () ->
                                    OssUploader.upload(
                                            httpClient,
                                            publicEndpoint,
                                            bucket,
                                            objectKey,
                                            sqlBytes,
                                            accessKeyId,
                                            accessKeySecret));
            System.out.println("[" + Console.timestamp() + "] SQL uploaded to: " + ossUrl);
            return ossUrl;
        } finally {
            httpClient.close();
        }
    }
}
