package ru.practicum.kafka_sprint_3;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.util.Collection;
import java.util.Map;

public class PrometheusSinkTask extends SinkTask {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private PrometheusHttpServer httpServer;

    public String version() {
        return "1.0.0";
    }

    @Override
    public void start(Map<String, String> props) {
        String baseUrl = props.get("prometheus.url");
        int port = Integer.parseInt(props.get("prometheus.port"));
        try {
            httpServer = PrometheusHttpServer.getInstance(baseUrl, port);
        } catch (Exception e) {
            throw new RuntimeException("Failed to start Prometheus HTTP server", e);
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        System.out.println("Received records: " + records);
        for (SinkRecord record : records) {
            try {
                System.out.println("Record value: " + record.value());
                String jsonString = MAPPER.writeValueAsString(record.value());
                System.out.println("Serialized JSON: " + jsonString);
                JsonNode rootNode = MAPPER.readTree(jsonString);



                rootNode.fields().forEachRemaining(entry -> {
                    String key = entry.getKey();
                    JsonNode metric = entry.getValue();

                    String name = metric.get("Name").asText();
                    String type = metric.get("Type").asText();
                    String description = metric.get("Description").asText();
                    double value = metric.get("Value").asDouble();

                    String prometheusData = String.format(
                        "# HELP %s %s\n# TYPE %s %s\n%s %f\n",
                        name, description, name, type, name, value
                    );

                    httpServer.addMetric(name, prometheusData);
                });
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void stop() { }

}
