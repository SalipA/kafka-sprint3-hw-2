package ru.practicum.kafka_sprint_3;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.List;
import java.util.Map;

public class PrometheusSinkConnector extends SinkConnector {

    private Map<String, String> props;

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public void start(Map<String, String> props) {
        this.props = props;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return PrometheusSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        return List.of(props);
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef()
            .define("prometheus.url", ConfigDef.Type.STRING, "http://localhost", ConfigDef.Importance.HIGH, "Base URL for Prometheus metrics")
            .define("prometheus.port", ConfigDef.Type.INT, 9877, ConfigDef.Importance.HIGH, "Port for Prometheus HTTP" +
                " server");
    }
}

