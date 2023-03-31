package de.inovex.airquality.data.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

class SensorDataTest {

    private static final String json = "{"
                + "\"sensordatavalues\": ["
                + "    {"
                + "        \"id\": 14630432104,"
                + "        \"value\": \"2.60\","
                + "       \"value_type\": \"P1\""
                + "    },"
                + "    {"
                + "        \"id\": 14630432105,"
                + "        \"value\": \"2.10\","
                + "       \"value_type\": \"P2\""
                + "    }"
                + "],"
                + "\"id\": 6675552884,"
                + "\"timestamp\": \"2021-08-27 14:30:59\""
                + "}";


    @Test
    public void testParseJsonToObject() throws IOException {
        final ObjectMapper objectMapper = new ObjectMapper();
        final SensorData data = objectMapper.readValue(json, SensorData.class);

        assertEquals(LocalDateTime.of(2021, 8, 27, 14, 30, 59), data.getTimestamp());
        assertEquals(2, data.getSensordatavalues().size());

        assertEquals(14630432104L, data.getSensordatavalues().get(0).getId());
        assertEquals(2.6, data.getSensordatavalues().get(0).getValue());
        assertEquals("P1", data.getSensordatavalues().get(0).getValue_type());
    }

    @Test
    public void testParseSensorData() {
        SensorData sensorDataA = new SensorData(
                1,
                LocalDateTime.parse("2021-08-27 14:30:59", SensorData.dateTimeFormatterIn),
                Collections.singletonList(new SensorData.SensorDataValue(1, 2,"abc"))
        );

        SensorData sensorDataB = SensorData.fromStruct(sensorDataA.toStruct());

        assertEquals(sensorDataA, sensorDataB);
    }

    @Test
    public void testKey() {
        final LocalDateTime d = LocalDateTime.of(2022, 3, 3, 3, 3, 3);
        final SensorData data = new SensorData(1L, d, Collections.emptyList());

        final long k = data.getKey();
        assertEquals(1646276400L, k);
    }

}