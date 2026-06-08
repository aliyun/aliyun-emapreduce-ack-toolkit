package com.aliyun.emr.ack;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

public class OssUploader {

    public static String upload(CloseableHttpClient httpClient, String endpoint, String bucket,
                                String objectKey, byte[] content,
                                String accessKeyId, String accessKeySecret) throws IOException {
        String contentType = "text/plain";
        byte[] md5Bytes;
        try {
            md5Bytes = MessageDigest.getInstance("MD5").digest(content);
        } catch (Exception e) {
            throw new IOException("Failed to compute MD5", e);
        }
        String contentMd5 = Base64.encodeBase64String(md5Bytes);

        SimpleDateFormat dateFormat = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z", Locale.US);
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        String date = dateFormat.format(new Date());

        String canonicalizedResource = "/" + bucket + "/" + objectKey;
        String signature = sign("PUT", contentMd5, contentType, date, canonicalizedResource, accessKeySecret);

        String url = "https://" + bucket + "." + endpoint + "/" + objectKey;
        HttpPut put = new HttpPut(url);
        put.setHeader(HttpHeaders.CONTENT_TYPE, contentType);
        put.setHeader("Content-MD5", contentMd5);
        put.setHeader("Date", date);
        put.setHeader(HttpHeaders.AUTHORIZATION, "OSS " + accessKeyId + ":" + signature);
        put.setEntity(new ByteArrayEntity(content));

        try (CloseableHttpResponse response = httpClient.execute(put)) {
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode < 200 || statusCode >= 300) {
                String body = response.getEntity() != null
                        ? EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8) : "";
                throw new HttpStatusException(statusCode, "OSS upload failed: HTTP " + statusCode + ", response: " + body);
            }
        }

        return "oss://" + bucket + "/" + objectKey;
    }

    static String sign(String method, String contentMd5, String contentType, String date,
                       String canonicalizedResource, String accessKeySecret) throws IOException {
        String stringToSign = method + "\n" + contentMd5 + "\n" + contentType + "\n" + date + "\n"
                + canonicalizedResource;
        try {
            Mac mac = Mac.getInstance("HmacSHA1");
            mac.init(new SecretKeySpec(accessKeySecret.getBytes(StandardCharsets.UTF_8), "HmacSHA1"));
            byte[] rawHmac = mac.doFinal(stringToSign.getBytes(StandardCharsets.UTF_8));
            return Base64.encodeBase64String(rawHmac);
        } catch (Exception e) {
            throw new IOException("Failed to sign OSS request", e);
        }
    }

    public static String[] parseOssPath(String ossPath) {
        if (ossPath == null || !ossPath.startsWith("oss://")) {
            return null;
        }
        String path = ossPath.substring("oss://".length());
        int slashIndex = path.indexOf('/');
        if (slashIndex <= 0) {
            return null;
        }
        String bucket = path.substring(0, slashIndex);
        String key = path.substring(slashIndex + 1);
        if (key.endsWith("/")) {
            key = key.substring(0, key.length() - 1);
        }
        return new String[]{bucket, key};
    }

    public static String toPublicEndpoint(String endpoint) {
        if (endpoint == null) return null;
        return endpoint.replace("-internal.aliyuncs.com", ".aliyuncs.com");
    }
}
