package edu.yu.cs.com3800.stage1;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import edu.yu.cs.com3800.JavaRunner;
import edu.yu.cs.com3800.SimpleServer;
import edu.yu.cs.com3800.Util;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class SimpleServerImpl implements SimpleServer {
    int port;
    HttpServer server;
    JavaRunner runner = new JavaRunner();
    static Logger logger;
    static FileHandler fh;

    public SimpleServerImpl(int port) throws IOException {
        logger = Logger.getLogger("LogFile");
        logger.setUseParentHandlers(false);
        fh = new FileHandler("./Logfile" + new GregorianCalendar().getTimeInMillis() + ".log", true);
        SimpleFormatter sf = new SimpleFormatter();
        fh.setFormatter(sf);
        logger.addHandler(fh);

        this.port = port;
    }

    static void main(String[] args)
    {
        int port = 9000;
        if(args.length >0)
        {
            port = Integer.parseInt(args[0]);
        }
        SimpleServer myserver = null;
        try
        {
            myserver = new SimpleServerImpl(port);
            myserver.start();
        }
        catch(Exception e)
        {
            logger.info(Util.getStackTrace(e));
            myserver.stop();
        }
    }

    public void start() {
        this.server = null;
        try {
            server = HttpServer.create(new InetSocketAddress(this.port), 100);
        } catch (IOException e) {
            logger.info(Util.getStackTrace(e));
        }
        assert server != null;
        server.createContext("/compileandrun", new Handler());
        server.setExecutor(null); // creates a default executor
        server.start();
    }

    public void stop() {
        fh.close();
        server.removeContext("/compileandrun");
        server.stop(0);
    }

    private class Handler implements HttpHandler {

        public Handler(){}

        public void handle(HttpExchange exchange) throws IOException {
            // Bad input handling
            List<String> type = exchange.getRequestHeaders().get("Content-Type");
            if(type == null || !type.contains("text/x-java-source")) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_REQUEST, 0);
                exchange.getResponseBody().write(new byte[0]);
                logger.info("HTTP_BAD_REQUEST: 400\nContent-Type is not text/x-java-source");
                exchange.close();
                return;
            }

            // Running the code
            String runnerResponse = "";
            String runnerErrorResponse = null;
            try {
                InputStream is = exchange.getRequestBody();
                byte[] bytes = is.readAllBytes();
                ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
                runnerResponse = runner.compileAndRun(byteArrayInputStream);
            } catch (Exception e) {
                String ret = e.getMessage() + "\n" + Util.getStackTrace(e);
                logger.info(ret);
                runnerErrorResponse = ret;
            }

            // Returning response
            if(runnerErrorResponse == null) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, runnerResponse.getBytes().length);
                exchange.getResponseBody().write(runnerResponse.getBytes());
                logger.info(HttpURLConnection.HTTP_OK + "\n" + runnerResponse);
            }
            else {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_REQUEST, runnerErrorResponse.getBytes().length);
                exchange.getResponseBody().write(runnerErrorResponse.getBytes());
                logger.info(HttpURLConnection.HTTP_BAD_REQUEST + "\n" + runnerErrorResponse);
            }
            exchange.close();
        }
    }
}
