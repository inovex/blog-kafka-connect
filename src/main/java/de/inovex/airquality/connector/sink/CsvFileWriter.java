package de.inovex.airquality.connector.sink;

import de.inovex.airquality.data.model.SensorData;

import java.time.LocalDateTime;

public interface CsvFileWriter {

    abstract class CsvWriterBuilder {

        protected LocalDateTime dateTimeHour;

        protected String outputDir;

        public CsvWriterBuilder dateTimeHour(LocalDateTime dateTimeHour) {
            this.dateTimeHour = dateTimeHour;
            return this;
        }

        public CsvWriterBuilder outputDir(String outputDir) {
            this.outputDir = outputDir;
            return this;
        }

        abstract public CsvFileWriter build() throws Exception;

    }

    void write(SensorData sensorData);

    void close();

}
