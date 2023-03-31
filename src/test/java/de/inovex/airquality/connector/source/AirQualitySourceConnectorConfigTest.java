package de.inovex.airquality.connector.source;

import de.inovex.airquality.connector.source.config.AirQualitySourceConnectorConfig;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class AirQualitySourceConnectorConfigTest {

    @Test
    void validateLocations() {
        final Map<String, String> nullValueProps = new HashMap<>();
        nullValueProps.put("locations", null);
        assertThrows(ConfigException.class, () -> new AirQualitySourceConnectorConfig(nullValueProps));

        final Map<String, String> invalidFormatProps = Map.of("locations", "1;2;3");
        assertThrows(ConfigException.class, () -> new AirQualitySourceConnectorConfig(invalidFormatProps));

        final Map<String, String> invalidDataProps = Map.of("locations", "foo;bar;baz;foo");
        assertThrows(ConfigException.class, () -> new AirQualitySourceConnectorConfig(invalidDataProps));

        final Map<String, String> invalidLongitudeProps = Map.of("locations", "100.0;0;0;0");
        assertThrows(ConfigException.class, () -> new AirQualitySourceConnectorConfig(invalidLongitudeProps));

        final Map<String, String> invalidLatitudeProps = Map.of("locations", "0;0;0;-190.0");
        assertThrows(ConfigException.class, () -> new AirQualitySourceConnectorConfig(invalidLatitudeProps));

        final Map<String, String> successfulProps = Map.of(
                "locations", "0;0;0;0",
                "topics", "topic"
        );
        assertDoesNotThrow(() -> new AirQualitySourceConnectorConfig(successfulProps));
    }

}