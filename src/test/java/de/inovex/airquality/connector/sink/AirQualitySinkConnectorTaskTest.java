package de.inovex.airquality.connector.sink;

import de.inovex.airquality.connector.sink.CsvFileWriter.CsvWriterBuilder;
import de.inovex.airquality.connector.sink.config.AirQualitySinkConnectorConfig;
import de.inovex.airquality.data.model.SensorData;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.*;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import static org.mockito.Mockito.*;


public class AirQualitySinkConnectorTaskTest {

    LocalDateTime dateTime = LocalDateTime.of(2022, Month.MARCH, 18, 15, 1, 18);

    static class MockCsvWriterBuilder implements Supplier<CsvWriterBuilder> {

        private final Map<LocalDateTime, CsvFileWriter> publishers;

        private MockCsvWriterBuilder(final Map<LocalDateTime, CsvFileWriter> publishers) {
            this.publishers = publishers;
        }

        static MockCsvWriterBuilder of(LocalDateTime localDateTime1, CsvFileWriter csvFileWriter1) {
            return new MockCsvWriterBuilder(Collections.singletonMap(localDateTime1, csvFileWriter1));
        }

        static MockCsvWriterBuilder of(LocalDateTime localDateTime1, CsvFileWriter csvFileWriter1, LocalDateTime localDateTime2, CsvFileWriter csvFileWriter2) {
            Map<LocalDateTime, CsvFileWriter> writerMap = new HashMap<>() {{
                put(localDateTime1, csvFileWriter1);
                put(localDateTime2, csvFileWriter2);
            }};
            return new MockCsvWriterBuilder(writerMap);
        }

        @Override
        public CsvWriterBuilder get() {
            return new CsvWriterBuilder() {

                @Override
                public CsvFileWriter build() {
                    return publishers.get(dateTimeHour);
                }
            };
        }
    }

    @Test
    void testNoEntry() {
        Collection<SinkRecord> records = Collections.emptyList();

        final CsvFileWriter csvFileWriter1 = mock(CsvFileWriter.class);

        final AirQualitySinkTask.TimeHandler th = mock(AirQualitySinkTask.TimeHandler.class);
        Mockito.when(th.getNow()).thenReturn(dateTime);

        AirQualitySinkTask task = new AirQualitySinkTask(th, MockCsvWriterBuilder.of(LocalDateTime.parse("2022-03-18T15:00:00", SensorData.dateTimeFormatterOutWithSec), csvFileWriter1));

        task.start(Map.of(AirQualitySinkConnectorConfig.OUTPUT_DIR, "/test"));
        task.put(records);

        Map<TopicPartition, OffsetAndMetadata> results = task.preCommit(Collections.singletonMap(new TopicPartition("topic", 0), new OffsetAndMetadata(0)));
        Assertions.assertEquals(results.size(), 0);

        verify(csvFileWriter1, times(0)).write(any());
        verify(csvFileWriter1, times(0)).close();
    }

    @Test
    void testNoCommit() {
        Collection<SinkRecord> records = IntStream.range(0, 10).mapToObj(i -> createRecord(
                "topic", 0, "key", 1,  "2022-03-18T15:36:00", 101, 21, "temperature", i, 102))
                .collect(Collectors.toList());

        final CsvFileWriter csvFileWriter1 = mock(CsvFileWriter.class);

        final AirQualitySinkTask.TimeHandler th = mock(AirQualitySinkTask.TimeHandler.class);
        Mockito.when(th.getNow()).thenReturn(dateTime);

        AirQualitySinkTask task = new AirQualitySinkTask(th, MockCsvWriterBuilder.of(LocalDateTime.parse("2022-03-18T15:00:00", SensorData.dateTimeFormatterOutWithSec), csvFileWriter1));

        task.start(Map.of(AirQualitySinkConnectorConfig.OUTPUT_DIR, "/test"));
        task.put(records);

        Map<TopicPartition, OffsetAndMetadata> results = task.preCommit(Collections.singletonMap(new TopicPartition("topic", 0), new OffsetAndMetadata(3)));
        Assertions.assertEquals(results.size(), 0);

        verify(csvFileWriter1, times(10)).write(any());
        verify(csvFileWriter1, times(0)).close();

    }

    // A (15:57) B (15:58) C (15:59) D (16:00) E (16:01)
    @Test
    public void testOneCommit() {
        Map<String, String> config = new HashMap<>();

        Collection<SinkRecord> records = Arrays.asList(
                createRecord("topic", 0, "key", 1,  "2022-03-18T14:57:00", 101, 21, "temperature", 0, 102),
                createRecord("topic", 0, "key", 1,  "2022-03-18T14:58:00", 101, 21, "temperature", 1, 102),
                createRecord("topic", 0, "key", 1,  "2022-03-18T14:59:00", 101, 21, "temperature", 2, 102),
                createRecord("topic", 0, "key", 1,  "2022-03-18T15:00:00", 101, 21, "temperature", 3, 102),
                createRecord("topic", 0, "key", 1,  "2022-03-18T15:01:00", 101, 21, "temperature", 4, 102)
        );

        final CsvFileWriter csvFileWriter1 = mock(CsvFileWriter.class);
        final CsvFileWriter csvFileWriter2 = mock(CsvFileWriter.class);

        final MockCsvWriterBuilder provider = MockCsvWriterBuilder.of(
                LocalDateTime.parse("2022-03-18T14:00:00", SensorData.dateTimeFormatterOutWithSec), csvFileWriter1,
                LocalDateTime.parse("2022-03-18T15:00:00", SensorData.dateTimeFormatterOutWithSec), csvFileWriter2
        );

        final AirQualitySinkTask.TimeHandler th = mock(AirQualitySinkTask.TimeHandler.class);
        Mockito.when(th.getNow()).thenReturn(dateTime);

        AirQualitySinkTask task = new AirQualitySinkTask(th, provider);

        task.start(Map.of(AirQualitySinkConnectorConfig.OUTPUT_DIR, "/test"));
        task.put(records);

        Map<TopicPartition, OffsetAndMetadata> results = task.preCommit(Collections.singletonMap(new TopicPartition("topic", 0), new OffsetAndMetadata(4)));
        Assertions.assertEquals(results.size(), 1);
        Assertions.assertEquals(results.get(new TopicPartition("topic", 0)).offset(), 2L);

        verify(csvFileWriter1, times(3)).write(any());
        verify(csvFileWriter2, times(2)).write(any());

        verify(csvFileWriter1, times(1)).close();
        verify(csvFileWriter2, times(0)).close();

    }

    private static SinkRecord createRecord(String topic, int partition, String key, long sensorId, String timestamp,
                                           long valueId, double value, String valueType, long offset, long valueId2) {
        Struct sensorDataValue1 = new Struct(SensorData.SensorDataValue.schema)
                .put(SensorData.SensorDataValue.ID_FIELD, valueId)
                .put(SensorData.SensorDataValue.VALUE_FIELD, value+1)
                .put(SensorData.SensorDataValue.VALUE_TYPE_FIELD, valueType);

        Struct sensorDataValue2 = new Struct(SensorData.SensorDataValue.schema)
                .put(SensorData.SensorDataValue.ID_FIELD, valueId2)
                .put(SensorData.SensorDataValue.VALUE_FIELD, value)
                .put(SensorData.SensorDataValue.VALUE_TYPE_FIELD, valueType);

        List<Struct> sensorArray = List.of(sensorDataValue1, sensorDataValue2);

        Struct messageValue = new Struct(SensorData.schema)
                .put(SensorData.ID_FIELD, sensorId)
                .put(SensorData.TIMESTAMP_FIELD, timestamp)
                .put(SensorData.SENSORDATAVALUES_FIELD, sensorArray);

        LocalDateTime localDateTime = LocalDateTime.parse(timestamp, SensorData.dateTimeFormatterOutWithSec);
        ZonedDateTime zdt = ZonedDateTime.of(localDateTime, ZoneId.of("Europe/Paris"));
        long date = zdt.toInstant().toEpochMilli();

        return new SinkRecord(
                topic,
                partition,
                Schema.STRING_SCHEMA,
                key,
                SensorData.schema,
                messageValue,
                offset,
                date,
                TimestampType.CREATE_TIME);
    }
}
