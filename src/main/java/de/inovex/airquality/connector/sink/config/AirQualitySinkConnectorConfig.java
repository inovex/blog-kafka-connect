package de.inovex.airquality.connector.sink.config;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class AirQualitySinkConnectorConfig extends AbstractConfig {

    public static final String OUTPUT_DIR = "file.output.dir";

    private static final String OUTPUT_DOC = "Folder where the csv-files shall be written";

    public AirQualitySinkConnectorConfig(Map<?, ?> originals) {
        super(conf(), originals);
    }

    public String getOutputDir() {
        return this.getString(OUTPUT_DIR);
    }

    public static ConfigDef conf() {
        return new ConfigDef().define(
                OUTPUT_DIR,
                ConfigDef.Type.STRING,
                ConfigDef.Importance.HIGH,
                OUTPUT_DOC
        );
    }
}
