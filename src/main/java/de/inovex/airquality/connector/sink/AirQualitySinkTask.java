package de.inovex.airquality.connector.sink;

import de.inovex.airquality.connector.sink.config.AirQualitySinkConnectorConfig;
import de.inovex.airquality.data.model.SensorData;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class AirQualitySinkTask extends SinkTask {
    private static final Logger logger = LoggerFactory.getLogger(AirQualitySinkTask.class);

    // needed for mocking time in unit tests
    public static class TimeHandler {
        public LocalDateTime getNow() {
            return LocalDateTime.now();
        }
    }

    private final TimeHandler timeHandler;

    private Supplier<CsvFileWriter.CsvWriterBuilder> csvWriterProvider;

    // we need one CsvWriter per hour per TopicPartition
    private final Map<TopicPartition, Map<LocalDateTime, CsvFileWriter>> topicDateHourWriter = new HashMap<>();

    private final Map<LocalDateTime, Long> latestOffset = new HashMap<>();

    private AirQualitySinkConnectorConfig config;

    // constructor for testing
    public AirQualitySinkTask(TimeHandler timeHandler,
                              final Supplier<CsvFileWriter.CsvWriterBuilder> csvWriterProvider) {
        this.timeHandler = timeHandler;
        this.csvWriterProvider = csvWriterProvider;
    }

    // kafka needs this no-arg constructor
    public AirQualitySinkTask() {this.timeHandler = new TimeHandler();}

    @Override
    public String version() {
        return "v0.0.1";
    }

    @Override
    public void start(Map<String, String> props) {
        logger.info("Start sink connector task");
        logger.info(props.toString());
        this.config = new AirQualitySinkConnectorConfig(props);
        // create new csvWriterProvider if not already set in constructor
        if(this.csvWriterProvider == null) {
            this.csvWriterProvider = AirQualityCsvFileWriter::builder;
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        logger.info("Start put");
        for (SinkRecord record : records) {

            // get topicPartition from record and add if not seen before
            final TopicPartition topicPartition = new TopicPartition(record.topic(), record.kafkaPartition());
            Map<LocalDateTime, CsvFileWriter> dateTimeHourWriter = topicDateHourWriter.computeIfAbsent(
                    topicPartition, tp -> new HashMap<>());

            // cast record to SensorData
            SensorData sensorData = SensorData.fromStruct((Struct) record.value());

            // cast ts to dateTimeHour
            LocalDateTime dateTimeHour = LocalDateTime.ofInstant(Instant.ofEpochMilli(record.timestamp()),
                    TimeZone.getTimeZone("Europe/Paris").toZoneId()).withMinute(0).withSecond(0).withNano(0);

            // write the data, if no appropriate csvWriter is available create new one
            dateTimeHourWriter.computeIfAbsent(dateTimeHour, k -> {
                try {
                    return csvWriterProvider.get()
                            .dateTimeHour(dateTimeHour)
                            .outputDir(config.getOutputDir())
                            .build();
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }).write(sensorData);

            // put the latest processed offset in latestOffset
            latestOffset.put(dateTimeHour, record.kafkaOffset());
        }
        logger.info("Put Done - {} records have been written", records.size());
    }

    @Override
    public void stop() {
        logger.warn("Stopping sink task");
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        logger.info("Start committing at offset: {}", currentOffsets);

        final Map<TopicPartition, OffsetAndMetadata> committableOffset;
        committableOffset = new HashMap<>();

        if (this.topicDateHourWriter.isEmpty()) {
            logger.info("Nothing has been written to topic yet");
            return committableOffset;
        }

        // get current hour from time
        LocalDateTime currentHour = timeHandler.getNow().withMinute(0).withSecond(0).withNano(0);

        for(TopicPartition topicPartition : currentOffsets.keySet()) {

            // get CsvWriter
            Map<LocalDateTime, CsvFileWriter> dateTimeHourWriter = this.topicDateHourWriter.get(topicPartition);

            // write csv when full hour changed, iterate over dateTimeHourWriter
            // get writer from past full hours
            Map<LocalDateTime, CsvFileWriter> pastDateHourWriter = dateTimeHourWriter.entrySet().stream()
                    .filter(e -> e.getKey().isBefore(currentHour))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            // get latest hour of past data
            Optional<LocalDateTime> lastPastHour = pastDateHourWriter.keySet().stream().max(LocalDateTime::compareTo);

            // if no pastHour available there is nothing to commit
            if (lastPastHour.isEmpty()) {
                logger.info("Nothing to commit");
                continue;
            }

            // get latest offset from latest past hour
            Long lastCommittableOffset = latestOffset.get(lastPastHour.get());

            // this offset is the last to commit
            committableOffset.put(topicPartition, new OffsetAndMetadata(lastCommittableOffset));

            // close all finished writers
            for(CsvFileWriter writer: pastDateHourWriter.values()){
                writer.close();
            }
        }

        logger.info("Committing following offsets: {}", committableOffset);
        return committableOffset;
    }
}
