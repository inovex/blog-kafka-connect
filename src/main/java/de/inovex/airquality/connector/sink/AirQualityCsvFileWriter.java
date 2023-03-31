package de.inovex.airquality.connector.sink;


import com.opencsv.CSVWriter;
import de.inovex.airquality.data.model.SensorData;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;

public class AirQualityCsvFileWriter implements CsvFileWriter {

    static class AirQualityCsvWriterBuilder extends CsvWriterBuilder {

        private AirQualityCsvWriterBuilder() { super(); }

        @Override
        public CsvFileWriter build() throws Exception {
            return new AirQualityCsvFileWriter(dateTimeHour, outputDir);
        }

    }

    private static final Logger logger = LoggerFactory.getLogger(AirQualityCsvFileWriter.class);
    private final LocalDateTime dateTimeHour;
    private final CSVWriter tmpCsvWriter;

    public AirQualityCsvFileWriter(LocalDateTime dateTimeHour, String outputDir) throws IOException {
        this.dateTimeHour = dateTimeHour;
        Files.createDirectories(Paths.get(outputDir));
        this.tmpCsvWriter = new CSVWriter(
                new FileWriter(
                        String.format("%s/sensordata_%s.csv",
                                outputDir,
                                dateTimeHour.toString().replace(":",""))),
                ';',
                CSVWriter.DEFAULT_QUOTE_CHARACTER,
                CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                CSVWriter.DEFAULT_LINE_END);
        tmpCsvWriter.writeNext(new String[]{"id", "timestamp", "sensordatavalues"});
        logger.info("Opened csv writer for hour {} at {}", this.dateTimeHour, new File("test.csv").getAbsolutePath());
    }

    @Override
    public void write(SensorData sensorData) {
        tmpCsvWriter.writeNext(sensorData.toArrayString());
    }

    @Override
    public void close() {
        try {
            tmpCsvWriter.close();
        } catch (IOException e) {
            throw new ConnectException("Can not close tmpCsvWriter", e);
        }
        logger.info("Closing csv writer for hour {}", this.dateTimeHour);
    }

    public LocalDateTime getDateTimeHour() {
        return dateTimeHour;
    }

    public static AirQualityCsvWriterBuilder builder() { return new AirQualityCsvWriterBuilder(); }

}
