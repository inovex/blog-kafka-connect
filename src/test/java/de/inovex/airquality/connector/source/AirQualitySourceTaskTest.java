package de.inovex.airquality.connector.source;

import de.inovex.airquality.connector.source.config.AirQualitySourceConnectorConfig;
import de.inovex.airquality.connector.source.config.BoundingBox;
import de.inovex.airquality.data.DataService;
import de.inovex.airquality.data.model.SensorData;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AirQualitySourceTaskTest {


    @Test
    public void testPoll() throws Exception {
        final DataService d = mock(DataService.class);
        when(d.getData()).thenReturn(createData(10));

        final SourceTaskContext context = getSourceTaskContextMock(null);

        Function<BoundingBox, DataService> dataServiceSupplier = (bb) -> d;
        final AirQualitySourceTask task = new AirQualitySourceTask(dataServiceSupplier);
        task.initialize(context);

        task.start(createConfig(
                "testTopic",
                new BoundingBox(1f,2f,3f,4f)
        ));
        final List<SourceRecord> poll = task.poll();

        assertEquals(10, poll.size());
    }


    @Test
    public void testPollWithoutOffsetFromContext() throws Exception {
        final DataService d = mock(DataService.class);
        List<SensorData> data = createData(15);

        when(d.getData())
                .thenReturn(data.subList(0, 10))
                .thenReturn(data.subList(5, 15));

        final SourceTaskContext context = getSourceTaskContextMock(null);

        Function<BoundingBox, DataService> dataServiceSupplier = (bb) -> d;
        final AirQualitySourceTask task = new AirQualitySourceTask(dataServiceSupplier);
        task.initialize(context);

        task.start(createConfig(
                "testTopic",
                new BoundingBox(1f,2f,3f,4f)
        ));
        final List<SourceRecord> first = task.poll();
        assertEquals(10, first.size());

        final List<SourceRecord> second = task.poll();
        assertEquals(5, second.size());

    }

    @Test
    public void testPollWithOffsetFromContext() throws Exception {
        final DataService d = mock(DataService.class);
        LocalDateTime now = LocalDateTime.now();
        List<SensorData> data = createData(15, now);

        when(d.getData())
                .thenReturn(data.subList(5, 15));

        final Map<String, Object> offset = new HashMap<>();
        offset.put("LAST_ID", String.valueOf(9L));
        offset.put("LAST_TIMESTAMP", now.plusSeconds(9L).toString());

        final SourceTaskContext context = getSourceTaskContextMock(offset);

        Function<BoundingBox, DataService> dataServiceSupplier = (bb) -> d;
        final AirQualitySourceTask task = new AirQualitySourceTask(dataServiceSupplier);
        task.initialize(context);

        task.start(createConfig(
                "testTopic",
                new BoundingBox(1f,2f,3f,4f)
        ));

        final List<SourceRecord> poll = task.poll();
        assertEquals(5, poll.size());
    }

    @Test
    public void testBoundingBox() {
        BoundingBox boundingBox = BoundingBox.fromString("0.0;0.0;0.0;0.0");
        assertEquals(boundingBox.getLat1(), 0.0);
        assertEquals(boundingBox.getLon1(), 0.0);
        assertEquals(boundingBox.getLat2(), 0.0);
        assertEquals(boundingBox.getLon2(), 0.0);
    }


    private SourceTaskContext getSourceTaskContextMock(final Map<String, Object> offsetMap) {
        final SourceTaskContext sourceTaskContext = mock(SourceTaskContext.class);
        final OffsetStorageReader offsetStorageReader = mock(OffsetStorageReader.class);

        when(offsetStorageReader.offset(any()))
                .thenReturn(offsetMap);

        when(sourceTaskContext.offsetStorageReader())
                .thenReturn(offsetStorageReader);

        return sourceTaskContext;
    }

    private static List<SensorData> createData(int amount) {
        return createData(amount, LocalDateTime.now());
    }

    private static List<SensorData> createData(int amount, LocalDateTime start) {
        return LongStream.range(0, amount).mapToObj(l -> new SensorData(l, start.plusSeconds(l), Collections.emptyList()))
                .collect(Collectors.toList());
    }

    private static Map<String, String> createConfig(final String topic, final BoundingBox boundingBox) {
        final Map<String, String> config = new HashMap<>();
        config.put(AirQualitySourceConnectorConfig.TOPIC_CONFIG, topic);
        config.put(AirQualitySourceConnectorConfig.LOCATIONS_CONFIG, boundingBox.toString());
        return config;
    }

}