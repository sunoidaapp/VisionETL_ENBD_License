package com.vision.examples;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

public class SparkAppKiller {
    public static void main(String[] args) {
        String sparkMasterUrl = "http://your-spark-master-url:6066";
        String appId = "your-spark-application-id";
        String killUrl = sparkMasterUrl + "/v1/submissions/kill/" + appId;
        HttpClient httpClient = HttpClients.createDefault();
        HttpPost httpPost = new HttpPost(killUrl);
        try {
            StringEntity entity = new StringEntity("");
            httpPost.setEntity(entity);
            httpPost.setHeader("Content-Type", "application/json");
            HttpResponse response = httpClient.execute(httpPost);
            // System.out.println("Response status: " + response.getStatusLine());
            String responseBody = EntityUtils.toString(response.getEntity());
            // System.out.println("Response body: " + responseBody);
        } catch (Exception e) {
            // e.printStackTrace();
        }
    }
}

