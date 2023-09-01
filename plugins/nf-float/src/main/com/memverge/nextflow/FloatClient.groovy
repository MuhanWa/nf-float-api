package com.memverge.nextflow

import java.lang.reflect.Type;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import okhttp3.*;

public class FloatClient {
    private static final MediaType JSON = MediaType.get("application/json; charset=utf-8");

    private final String username
    private final String password
    private String url

    public FloatClient(String username, String password, String url) {
        this.username = username;
        this.password = password;
        this.url = url;
    }

    private OkHttpClient getClient() {
        final TrustManager[] trustAllCerts = new TrustManager[] {
                new X509TrustManager() {
                    @Override
                    public void checkClientTrusted(java.security.cert.X509Certificate[] chain, String authType)
                            throws CertificateException {
                    }

                    @Override
                    public void checkServerTrusted(java.security.cert.X509Certificate[] chain, String authType)
                            throws CertificateException {
                    }

                    @Override
                    public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                        return new X509Certificate[0];
                    }
                }
        };
        // Install the all-trusting trust manager
        SSLContext sslContext;
        try {
            sslContext = SSLContext.getInstance("SSL");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return null;
        }
        try {
            sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
        } catch (KeyManagementException e) {
            e.printStackTrace();
            return null;
        }
        // Create an ssl socket factory with our all-trusting manager
        final SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();
        return new OkHttpClient.Builder()
                .sslSocketFactory(sslSocketFactory, (X509TrustManager) trustAllCerts[0])
                .hostnameVerifier((hostname, session) -> true)
                .connectTimeout(30, TimeUnit.SECONDS)
                .writeTimeout(30, TimeUnit.SECONDS)
                .readTimeout(30, TimeUnit.SECONDS)
                .build();
    }

    public String getUsername(){
        return username;
    }

    public String getPassword(){
        return password;
    }

    public String getUrl(){
        return url;
    }

    public void setUrl(String url){
        this.url = url
    }

    public String getToken() {
        final String auth = username + ":" + password;
        final String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes());

        OkHttpClient client = getClient();
        Request request = new Request.Builder()
                .url(url + "/api/v1/login")
                .header("Authorization", "Basic " + encodedAuth)
                .build();

        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                System.err.println("failed to get token, Server returned HTTP error code " + response.code());
                System.err.println(response.body().string());
            }
            return response.header("authorization");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    public boolean isValidated(){
        try{
            if(!getToken().isEmpty()){
                return true;
            }
        } catch(Exception e){
            e.printStackTrace();
        }
        return false;
    }

    public void cancelJob(String jobID){
        final String token = getToken();
        Request request = new Request.Builder()
                .url(url + "/api/v1/jobs/" + jobID)
                .header("Authorization", token)
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .delete()
                .build();
        
        try (Response response = getClient().newCall(request).execute()) {
            if (!response.isSuccessful()) {
                System.err.println("failed to cancel job, Server returned HTTP error code " + response.code());
                System.err.println(response.body().string());
            }
            System.out.println("Job " + jobID + " cancelled successfully");
            return;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return;
    }

    public String submitJob (String specString) throws Exception {
        final String token = getToken();
        Gson gson = new Gson();
        RequestBody body = RequestBody.create(specString, JSON);
        Request request = new Request.Builder()
                .url(url + "/api/v1/jobs")
                .header("Authorization", token)
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .post(body)
                .build();

        try (Response response = getClient().newCall(request).execute()) {
            if (!response.isSuccessful()) {
                System.err.println("failed to submit job, Server returned HTTP error code " + response.code());
                System.err.println(response.body().string());
            }
            String responseJson = response.body().string();
            Gson responseGson = new Gson();
            JobRest jobRest = responseGson.fromJson(responseJson, JobRest.class);
            return jobRest.id;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    public JobRest getJobStatus(String jobId) {
        final String token = getToken();
        Request request = new Request.Builder()
                .url(String.format("%s/api/v1/jobs/%s",url,jobId))
                .header("Authorization", token)
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .build();

        try (Response response = getClient().newCall(request).execute()) {
            if (!response.isSuccessful()) {
                System.err.println("failed to get job status, Server returned HTTP error code " + response.code());
                System.err.println(response.body().string());
            }
            String responseJson = response.body().string();
            Gson responseGson = new Gson();
            JobRest jr = responseGson.fromJson(responseJson, JobRest.class)
            return jr
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public String getJobList(){
        final String token = getToken();
        Request request = new Request.Builder()
                .url(String.format("%s/api/v1/jobPages",url))
                .header("Authorization", token)
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .build();
        
        try (Response response = getClient().newCall(request).execute()) {
            if (!response.isSuccessful()) {
                System.err.println("failed to get job status, Server returned HTTP error code " + response.code());
                System.err.println(response.body().string());
                return null
            }
            String responseJson = response.body().string();
            return responseJson
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null
    }
}