package de.inovex.airquality.connector.source;

import de.inovex.airquality.connector.source.config.AirQualitySourceConnectorConfig;
import de.inovex.airquality.connector.source.config.BoundingBox;
import de.inovex.airquality.data.DataService;
import de.inovex.airquality.data.model.SensorData;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * The AirQualitySourceTask ingests air quality sensor data for one or more locations.
 * The locations are determined by the connector using the task config map.
 *
 * In each poll, the task retrieves sensor data from all configured locations.
 * The offsets are stored as configs containing the ID and the timestamp of the
 * last record read from the API. This enables removing duplicates.
 */
public class AirQualitySourceTask extends SourceTask {
    private static final Logger logger = LoggerFactory.getLogger(AirQualitySourceTask.class);

    private final Function<BoundingBox, DataService> dataServiceSupplier;
    private String topic;
    private AirQualitySourceConnectorConfig config;

    private List<AirQualitySourcePartition> airQualityPartitions;

    public AirQualitySourceTask() {
        super();
        this.dataServiceSupplier = (bb) -> {
            try {
                return DataService.builder().boundingBox(bb).build();
            } catch (URISyntaxException e) {
                throw new ConnectException(e);
            }
        };
    }

    public AirQualitySourceTask(final Function<BoundingBox, DataService> dataServiceSupplier) {
        super();
        this.dataServiceSupplier = dataServiceSupplier;
    }

    @Override
    public String version() {
        return "v0.0.1";
    }

    /**
     * starts the task using the given config
     * @param props the config
     */
    @Override
    public void start(Map<String, String> props) {
        logger.info("Start source connector task");
        logger.info(String.valueOf(props));

        config = new AirQualitySourceConnectorConfig(props);
        topic = config.getTopic();
        OffsetStorageReader offsetStorageReader = context.offsetStorageReader();

        // Create an AirQualityPartition for each configured location
        airQualityPartitions = config.getLocations().stream()
                .map(loc -> createAirQualityPartition(loc, offsetStorageReader))
                .collect(Collectors.toList());

        logger.info("Source connector started");
    }

    /**
     * Create an AirQualityPartition for the given location and sets its offset using the given OffsetStorageReader
     * @param location the location as a String
     * @param offsetStorageReader the OffsetStorageReader
     * @return the AirQualitySourcePartition
     */
    private AirQualitySourcePartition createAirQualityPartition(String location, OffsetStorageReader offsetStorageReader) {
        BoundingBox bbox = BoundingBox.fromString(location);
        AirQualitySourcePartition airQualityPartition = new AirQualitySourcePartition(bbox,
                this.dataServiceSupplier.apply(bbox));
        Map<String, Object> offset = offsetStorageReader.offset(airQualityPartition.getSourcePartition());
        airQualityPartition.setSourceOffset(offset);
        return airQualityPartition;
    }


    /**
     * Polls for new data by retrieving sensor data for each location configured for this task
     *
     * @return the polled SourceRecords
     * @throws InterruptedException
     */
    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        try {
            logger.info("Start poll");
            List<SourceRecord> sourceRecords = new ArrayList<>();
            for(AirQualitySourcePartition airQualityPartition : airQualityPartitions) {
                pollSourcePartition(airQualityPartition, sourceRecords);
            }
            logger.info("Poll Done - {} total rows were written to topic", sourceRecords.size());
            return sourceRecords;
        } catch (IOException e) {
            throw new ConnectException(e);
        }
    }

    /**
     * retrieves sensor data for one source partition / location and appends the source records
     * to the given list.
     *
     * @param airQualityPartition the partition for which data is retrieved
     * @param sourceRecords the list to which records are appended
     * @throws IOException
     * @throws InterruptedException
     */
    private void pollSourcePartition(AirQualitySourcePartition airQualityPartition, List<SourceRecord> sourceRecords)
            throws IOException, InterruptedException {
        List<SensorData> data = getLastData(airQualityPartition);
        logger.info("{} rows were fetched by poll for Bounding Box {}", data.size(),
                airQualityPartition.getLocation());

        for (SensorData sensorData : data) {
            airQualityPartition.setLastID(sensorData.getId());
            airQualityPartition.setLastTimeStamp(sensorData.getTimestamp());
            sourceRecords.add(
                    new SourceRecord(
                            airQualityPartition.getSourcePartition(),
                            airQualityPartition.getSourceOffset(),
                            topic,
                            Schema.INT64_SCHEMA,
                            sensorData.getKey(),
                            SensorData.schema, sensorData.toStruct()
                    )
            );
        }
    }

    /***
     * returns the sensor data for the given source partition and removes any duplicates using the partitions offset.
     *
     * @param airQualityPartition the source partition for which data is returned
     * @return the deduplicated source records
     * @throws IOException
     * @throws InterruptedException
     */
    private List<SensorData> getLastData(AirQualitySourcePartition airQualityPartition) throws IOException, InterruptedException {
        List<SensorData> data = airQualityPartition.getDataService().getData();
        int idx = IntStream.range(0, data.size())
                .filter(i -> Objects.equals(data.get(i).getId(), airQualityPartition.getLastID())
                        && Objects.equals(data.get(i).getTimestamp(), airQualityPartition.getLastTimeStamp()))
                .map(i -> i+1)
                .findFirst()
                .orElse(0);

        return data.subList(idx, data.size());
    }

    @Override
    public void stop() {
        logger.warn("Stopping source task");
    }

}
