package de.inovex.airquality.connector.sink;

import de.inovex.airquality.connector.sink.config.AirQualitySinkConnectorConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class AirQualitySinkConnector extends SinkConnector {
    private static final Logger logger = LoggerFactory.getLogger(AirQualitySinkTask.class);

    private Map<String, String> settings;

    @Override
    public void start(Map<String, String> settings) {
        logger.info("Connector has started");
        this.settings = settings;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return AirQualitySinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        return List.of(settings);
    }

    @Override
    public void stop() {
        logger.warn("Connector was stopped");
    }

    @Override
    public ConfigDef config() {
        return AirQualitySinkConnectorConfig.conf();
    }

    @Override
    public String version() {
        return "v1.0.0";
    }
}
