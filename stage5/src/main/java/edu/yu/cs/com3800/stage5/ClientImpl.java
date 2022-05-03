package edu.yu.cs.com3800.stage5;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class ClientImpl implements Client {
    String hostName;
    int hostPort;
    Response response;
    int n = 0;
    private final HttpClient httpClient = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_2)
            .build();

    public ClientImpl(String hostName, int hostPort) throws MalformedURLException {
        this.hostName = hostName;
        this.hostPort = hostPort;
    }

    public void sendCompileAndRunRequest(String src, String destinationHostName, int destinationPort) throws IOException {
//        System.out.println("Entered sendRequest method");
        if(src == null) {
            throw new IllegalArgumentException("Request cannot be null");
        }
        HttpRequest request = HttpRequest.newBuilder()
                .POST(HttpRequest.BodyPublishers.ofString(src))
                .uri(URI.create("http://" + destinationHostName + ":" + destinationPort + "/compileandrun"))
                .setHeader("Content-Type", "text/x-java-source")
                .version(HttpClient.Version.HTTP_2)
                .build();

        try {
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
//            System.out.println("Sent request #" + ++n);
            this.response = new Response(response.statusCode(), response.body());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void sendCompileAndRunRequestWithKill(String src, String destinationHostName, int destinationPort) throws IOException {
//        System.out.println("Entered sendRequest method");
        if(src == null) {
            throw new IllegalArgumentException("Request cannot be null");
        }
        HttpRequest request = HttpRequest.newBuilder()
                .POST(HttpRequest.BodyPublishers.ofString(src))
                .uri(URI.create("http://" + destinationHostName + ":" + destinationPort + "/compileandrun"))
                .setHeader("Content-Type", "text/x-java-source")
                .version(HttpClient.Version.HTTP_2)
                .build();

        try {
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
//            System.out.println("Sent request #" + ++n);
            this.response = new Response(response.statusCode(), response.body());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Response getResponse() throws IOException {
        return this.response;
    }
}
