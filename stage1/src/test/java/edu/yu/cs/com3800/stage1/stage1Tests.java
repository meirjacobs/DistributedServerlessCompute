package edu.yu.cs.com3800.stage1;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.*;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class stage1Tests {
    static SimpleServerImpl server;

    @BeforeAll
    static void starter() throws IOException {
        server = new SimpleServerImpl(9000);
        server.start();
        System.out.println("NOTE: The log files are under the root directory");
    }

    @BeforeEach
    public void lineSeparator() {
        System.out.println("----------------------------------------------------------------------------");
    }

    @Test
    public void simpleTest() throws IOException {
        System.out.println("\nBegin Simple Test\n");
        String src = "public class DummyClass {public String run() {return \"It's Working!!!\";}}";
        ClientImpl client = new ClientImpl("http://localhost:", 9000);
        client.sendCompileAndRunRequest(src);
        Client.Response response = client.getResponse();
        String expectedResponse = "200\nIt's Working!!!";
        String actualResponse = response.getCode() + "\n" + response.getBody();

        System.out.println("Expected response:");
        System.out.println(expectedResponse);
        System.out.println("\nActual response:");
        System.out.println(actualResponse);

        assertTrue(actualResponse.startsWith(expectedResponse.trim()));
        //assertEquals(expectedResponse.trim(), actualResponse.trim());
    }

    @Test
    public void runMethodReturnsInt() throws IOException {
        System.out.println("\nBegin runMethodReturnsInt Test\n");
        String src = "public class DummyClass {public int run() {return 613;}}";
        ClientImpl client = new ClientImpl("http://localhost:", 9000);
        client.sendCompileAndRunRequest(src);
        Client.Response response = client.getResponse();
        String expectedResponse = "400\nThe return type of the class was int, not java.lang.String";
        String actualResponse = response.getCode() + "\n" + response.getBody();

        System.out.println("Expected response:");
        System.out.println(expectedResponse);
        System.out.println("\nActual response:");
        System.out.println(actualResponse);

        assertTrue(actualResponse.startsWith(expectedResponse.trim()));
        //assertEquals(expectedResponse.trim(), actualResponse.trim());
    }

    @Test
    public void noRunMethod() throws IOException {
        System.out.println("\nBegin noRunMethod Test\n");
        String src = "public class DummyClass {public int walk() {return 613;}}";
        ClientImpl client = new ClientImpl("http://localhost:", 9000);
        client.sendCompileAndRunRequest(src);
        Client.Response response = client.getResponse();
        String expectedResponse = "400\nCould not create and run instance of class";
        String actualResponse = response.getCode() + "\n" + response.getBody();

        System.out.println("Expected response:");
        System.out.println(expectedResponse);
        System.out.println("\nActual response:");
        System.out.println(actualResponse);

        assertTrue(actualResponse.startsWith(expectedResponse.trim()));
        //assertEquals(expectedResponse.trim(), actualResponse.trim());
    }

    @Test
    public void argsConstructor() throws IOException {
        System.out.println("\nBegin argsConstructor Test\n");
        String src = "public class DummyClass {public DummyClass(int number){number++;} public int walk() {return 613;}}";
        ClientImpl client = new ClientImpl("http://localhost:", 9000);
        client.sendCompileAndRunRequest(src);
        Client.Response response = client.getResponse();
        String expectedResponse = "400\nCould not create and run instance of class";
        String actualResponse = response.getCode() + "\n" + response.getBody();

        System.out.println("Expected response:");
        System.out.println(expectedResponse);
        System.out.println("\nActual response:");
        System.out.println(actualResponse);

        assertTrue(actualResponse.startsWith(expectedResponse.trim()));
        //assertEquals(expectedResponse.trim(), actualResponse.trim());
    }

    @Test
    public void runMethodTakesArgument() throws IOException {
        System.out.println("\nBegin runMethodTakesArgument Test\n");
        String src = "public class DummyClass {public String run(String s) {return s;}}";
        ClientImpl client = new ClientImpl("http://localhost:", 9000);
        client.sendCompileAndRunRequest(src);
        Client.Response response = client.getResponse();
        String expectedResponse = "400\nCould not create and run instance of class";
        String actualResponse = response.getCode() + "\n" + response.getBody();

        System.out.println("Expected response:");
        System.out.println(expectedResponse);
        System.out.println("\nActual response:");
        System.out.println(actualResponse);

        assertTrue(actualResponse.startsWith(expectedResponse.trim()));
        //assertEquals(expectedResponse.trim(), actualResponse.trim());
    }

    @Test
    public void javaCodeShouldntCompile() throws IOException {
        System.out.println("\nBegin javaCodeShouldntCompile Test\n");
        String src = "public class DummyClass {\n\npublic String run() {\nreturn 613;\n}\n}\n";
        ClientImpl client = new ClientImpl("http://localhost:", 9000);
        client.sendCompileAndRunRequest(src);
        Client.Response response = client.getResponse();
        String expectedResponse = "400\nCode did not compile:\nError on line 4, column 8 in string:///DummyClass.java\n";
        String actualResponse = response.getCode() + "\n" + response.getBody();

        System.out.println("Expected response:");
        System.out.println(expectedResponse);
        System.out.println("\nActual response:");
        System.out.println(actualResponse);
        assertTrue(actualResponse.startsWith(expectedResponse.trim()));
        //assertEquals(expectedResponse.trim(), actualResponse.trim());
    }

    @Test
    public void newlineCharacters() throws IOException {
        System.out.println("\nBegin newlineCharacters Test\n");
        String src = "public class DummyClass {\n\npublic String run() {\nreturn \"It's Working!!!\";\n}\n}\n";
        ClientImpl client = new ClientImpl("http://localhost:", 9000);
        client.sendCompileAndRunRequest(src);
        Client.Response response = client.getResponse();
        String expectedResponse = "200\nIt's Working!!!";
        String actualResponse = response.getCode() + "\n" + response.getBody();

        System.out.println("Expected response:");
        System.out.println(expectedResponse);
        System.out.println("\nActual response:");
        System.out.println(actualResponse);

        assertTrue(actualResponse.startsWith(expectedResponse.trim()));
        //assertEquals(expectedResponse.trim(), actualResponse.trim());
    }

    @Test
    public void badClient() throws IOException {
        System.out.println("\nBegin BadClient Test\n");
        String src = "public class DummyClass {\n\npublic String run() {\nreturn \"It's Working!!!\";\n}\n}\n";
        BadClient client = new BadClient("http://localhost:", 9000);
        client.sendCompileAndRunRequest(src);
        Client.Response response = client.getResponse();
        String expectedResponse = "400\n";
        String actualResponse = response.getCode() + "\n" + response.getBody();

        System.out.println("Expected response:");
        System.out.println(expectedResponse);
        System.out.println("\nActual response:");
        System.out.println(actualResponse);

        assertTrue(actualResponse.startsWith(expectedResponse.trim()));
        //assertEquals(expectedResponse.trim(), actualResponse.trim());
    }

    /*@Test
    public void purposelyFail() {
        assertEquals(true, false);
    }*/

    @AfterAll
    public static void ender() {
        server.stop();
    }
}

class BadClient implements Client{
    String hostName;
    int hostPort;
    Response response;
    private final HttpClient httpClient = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_2)
            .build();

    public BadClient(String hostName, int hostPort) throws MalformedURLException {
        this.hostName = hostName;
        this.hostPort = hostPort;
    }

    public void sendCompileAndRunRequest(String src) throws IOException {
        HttpRequest request = HttpRequest.newBuilder()
                .POST(HttpRequest.BodyPublishers.ofString(src))
                .uri(URI.create(this.hostName + this.hostPort + "/compileandrun"))
                .setHeader("Content-Type", "NOT-text/x-java-source")
                .version(HttpClient.Version.HTTP_2)
                .build();

        try {
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            this.response = new Response(response.statusCode(), response.body());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Response getResponse() throws IOException {
        return this.response;
    }
}
