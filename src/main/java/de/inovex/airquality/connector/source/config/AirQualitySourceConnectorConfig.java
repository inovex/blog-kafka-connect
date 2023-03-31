package de.inovex.airquality.connector.source.config;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.List;
import java.util.Map;

public class AirQualitySourceConnectorConfig extends AbstractConfig {

    public static final String LOCATIONS_CONFIG = "locations";

    public static final String LOCATIONS_DOC = "A comma-separated list of locations given as bounding boxes. " +
            "A bounding box is specified by two geo-coordinates in the format lat1;long1;lat2;long2 ";

    public static final String TOPIC_CONFIG = "topics";

    public static final String TOPIC_DOC = "Topic where the data shall be written";

    public List<String> getLocations() {
        return this.getList(LOCATIONS_CONFIG);
    }

    public String getTopic() {
        return this.getString(TOPIC_CONFIG);
    }

    public AirQualitySourceConnectorConfig(Map<?, ?> originals) {
        super(conf(), originals);
    }


    public static ConfigDef conf() {
        return new ConfigDef()
                .define(
                        LOCATIONS_CONFIG,
                        ConfigDef.Type.LIST,
                        ConfigDef.NO_DEFAULT_VALUE,
                        new BoundingBoxValidator(),
                        ConfigDef.Importance.HIGH,
                        LOCATIONS_DOC
                )
                .define(
                        TOPIC_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        TOPIC_DOC
                );
    }


    private static class BoundingBoxValidator implements ConfigDef.Validator {

        @Override
        @SuppressWarnings("unchecked")
        public void ensureValid(String name, Object value) {
            if (value == null) {
                throw new ConfigException(name, "cannot be null");
            }

            final List<String> locations = (List<String>) value;

            for (String bbox: locations) {
                final String[] parts = bbox.split(";");
                if (parts.length != 4) {
                    throw new ConfigException(name, value, "has wrong format - one");
                }
                for(int i = 0; i<parts.length; i++) {
                    String part = parts[i];
                    float val;
                    try {
                        val = Float.parseFloat(part);
                    } catch (NumberFormatException e) {
                        throw new ConfigException(name, part, "has wrong format - two");
                    }
                    if(i % 2 == 0 && val > 90.0f || val < -90.0f) {
                        throw new ConfigException(name, val, "latitude must be between -90.0 and 90.0");
                    }
                    if(i % 2 == 1 && val > 180.0f || val < -180.0f) {
                        throw new ConfigException(name, val, "longitude must be between -180.0 and 180.0");
                    }
                }
            }
        }
    }
}
