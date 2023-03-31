package de.inovex.airquality.connector.source;

import de.inovex.airquality.connector.source.config.AirQualitySourceConnectorConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Source connector which ingests air quality sensor data.
 *
 * The connector is configured to read from one or more locations.
 * A location is a bounding box of geo-coordinates.
 *
 * Each configured location represents one source partition.
 * Locations define the level of parallelism, as they are distributed among the connector tasks.
 *
 */
public class AirQualitySourceConnector extends SourceConnector {
    private static final Logger logger = LoggerFactory.getLogger(AirQualitySourceConnector.class);

    private AirQualitySourceConnectorConfig config;

    @Override
    public void start(Map<String, String> props) {
        logger.info("Connector has started");
        config = new AirQualitySourceConnectorConfig(props);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return AirQualitySourceTask.class;
    }

    /**
     * Creates the configs for the connector tasks.
     * It distributes all configured locations evenly among a maximum of maxTasks.
     *
     * @param maxTasks the maximum number of tasks
     * @return the configs for the connector tasks
     */
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        return ConnectorUtils.groupPartitions(config.getLocations(), maxTasks).stream()
                .map(this::createTaskConfig).collect(Collectors.toList());
    }

    private Map<String, String> createTaskConfig(List<String> locations) {
        HashMap<String, String> taskConfig = new HashMap<>(config.originalsStrings());
        taskConfig.put("locations", String.join(",", locations));
        return taskConfig;
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return AirQualitySourceConnectorConfig.conf();
    }

    @Override
    public String version() {
        return "1.0-SNAPSHOT";
    }
}
