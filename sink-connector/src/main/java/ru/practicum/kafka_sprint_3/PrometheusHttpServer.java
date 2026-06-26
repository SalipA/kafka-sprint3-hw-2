package ru.practicum.kafka_sprint_3;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.ConcurrentHashMap;

public class PrometheusHttpServer {
    private static PrometheusHttpServer instance;
    private String baseUrl;

    private PrometheusHttpServer() {}

    private final ConcurrentHashMap<String, String> metrics = new ConcurrentHashMap<>();

    public static PrometheusHttpServer getInstance(String url, int port) throws Exception {
        if (instance == null) {
            instance = new PrometheusHttpServer();
            instance.baseUrl = url;
            instance.start(port);
        }
        return instance;
    }

    public void start(int port) throws Exception {
        HttpServer server = HttpServer.create(new java.net.InetSocketAddress("0.0.0.0", port), 0);
        server.createContext("/metrics", new MetricsHandler(this));
        server.start();
        System.out.println("Server started at: http://localhost:" + port + "/metrics");
    }

    public void addMetric(String name, String data) {
        metrics.put(name, data);
    }

    public ConcurrentHashMap<String, String> getMetrics() {
        return metrics;
    }

    // HTTP Handler
    private static class MetricsHandler implements HttpHandler {
        private final PrometheusHttpServer server;

        public MetricsHandler(PrometheusHttpServer server) {
            this.server = server;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            System.out.println("Received request for /metrics");

            ConcurrentHashMap<String, String> metrics = server.getMetrics();

            String response = "# Base URL: " + server.baseUrl + "\n";
            for (String metric : metrics.values()) {
                response += metric + "\n";
            }

            exchange.sendResponseHeaders(200, response.getBytes().length);
            OutputStream os = exchange.getResponseBody();
            os.write(response.getBytes());
            os.close();
        }
    }
}

