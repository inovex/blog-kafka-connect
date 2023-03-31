package de.inovex.airquality.data.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateTimeSerializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@JsonIgnoreProperties(ignoreUnknown=true)
public class SensorData {
    private static final Logger logger = LoggerFactory.getLogger(SensorData.class);

    public static class SensorDataValue {

        private final long id;

        private final double value;

        private final String value_type;

        public long getId() {
            return id;
        }

        public double getValue() {
            return value;
        }

        public String getValue_type() {
            return value_type;
        }

        public static String ID_FIELD = "id";

        public static String VALUE_FIELD = "value";

        public static String VALUE_TYPE_FIELD = "value_type";

        public static Schema schema = SchemaBuilder.struct()
                .field(ID_FIELD, Schema.INT64_SCHEMA)
                .field(VALUE_FIELD, Schema.FLOAT64_SCHEMA)
                .field(VALUE_TYPE_FIELD, Schema.STRING_SCHEMA)
                .build();

        @JsonCreator
        public SensorDataValue(@JsonProperty("id") long id,
                               @JsonProperty("value") double value,
                               @JsonProperty("value_type") String value_type) {
            this.id = id;
            this.value = value;
            this.value_type = value_type;
        }

        public Struct toStruct() {
            return new Struct(schema)
                    .put(ID_FIELD, id)
                    .put(VALUE_FIELD, value)
                    .put(VALUE_TYPE_FIELD, value_type);
        }

        public static SensorDataValue fromStruct(Struct struct) {
            return new SensorDataValue(
                    struct.getInt64("id"),
                    struct.getFloat64("value"),
                    struct.getString("value_type")
            );
        }

        public String toString(){
            return "{SensorId:" + getId() + ", SensorValue: " + getValue() + ", SensorValueType: " + getValue_type() + "}";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SensorDataValue that = (SensorDataValue) o;
            return Objects.equals(id, that.id) && Objects.equals(value, that.value) && Objects.equals(value_type, that.value_type);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, value, value_type);
        }
    }

    private final long id;

    @JsonFormat(shape= JsonFormat.Shape.STRING, pattern="yyyy-MM-dd HH:mm:ss", timezone = "UTC")
    @JsonDeserialize(using=LocalDateTimeDeserializer.class)
    @JsonSerialize(using=LocalDateTimeSerializer.class)
    private final LocalDateTime timestamp;

    private final List<SensorDataValue> sensordatavalues;

    public static String ID_FIELD = "id";

    public static String TIMESTAMP_FIELD = "timestamp";

    public static String SENSORDATAVALUES_FIELD = "sensordatavalues";

    public static DateTimeFormatter dateTimeFormatterIn = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static DateTimeFormatter dateTimeFormatterOutWithSec = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
    public static DateTimeFormatter dateTimeFormatterOutNoSec = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm");

    public static Schema schema = SchemaBuilder.struct()
            .name("de.inovex.airquality.data.model.SensorData").version(1).doc("TODO")
            .field(ID_FIELD, Schema.INT64_SCHEMA)
            .field(TIMESTAMP_FIELD, Schema.STRING_SCHEMA)
            .field(SENSORDATAVALUES_FIELD, SchemaBuilder.array(SensorDataValue.schema))
            .build();

    @JsonCreator
    public SensorData(@JsonProperty("id") long id,
                      @JsonProperty("timestamp") LocalDateTime timestamp,
                      @JsonProperty("sensordatavalues") List<SensorDataValue> sensordatavalues) {
        this.id = id;
        this.timestamp = timestamp;
        this.sensordatavalues = sensordatavalues;
    }

    public Long getId() {
        return id;
    }

    public LocalDateTime getTimestamp() {
        return timestamp.atOffset(ZoneOffset.UTC).toLocalDateTime();
    }

    public long getKey() {
        return getTimestamp()
                .withMinute(0)
                .withSecond(0)
                .withNano(0)
                .toEpochSecond(ZoneOffset.UTC);
    }

    public List<SensorDataValue> getSensordatavalues() {
        return sensordatavalues;
    }

    public Struct toStruct() {
        return new Struct(schema)
                .put(ID_FIELD, id)
                .put(TIMESTAMP_FIELD, getTimestamp().toString())
                .put(SENSORDATAVALUES_FIELD,
                        sensordatavalues.stream().map(SensorDataValue::toStruct).collect(Collectors.toList()));
    }

    public String[] toArrayString(){

        List<String> list = new ArrayList<>();
        list.add(String.valueOf(id));
        list.add(getTimestamp().toString());
        list.add(String.format("[%s]", sensordatavalues.stream().map(SensorDataValue::toString).collect(Collectors.joining(","))));

        return list.toArray(new String[0]);
    }

    public static SensorData fromStruct(Struct struct) {
        String timestamp = struct.getString("timestamp");
        LocalDateTime timestampConverted;
        if (timestamp.length() == 16){
            timestampConverted = LocalDateTime.parse(timestamp, dateTimeFormatterOutNoSec);
        } else if (timestamp.length() == 19){
            timestampConverted = LocalDateTime.parse(timestamp, dateTimeFormatterOutWithSec);
        } else {
            logger.error("timestamp format is unknown. Timestamp: {}", timestamp);
            timestampConverted = LocalDateTime.parse(timestamp, dateTimeFormatterOutWithSec);
        }
        return new SensorData(
                struct.getInt64("id"),
                timestampConverted,
                struct.getArray("sensordatavalues").stream().map(e -> SensorDataValue.fromStruct((Struct) e)).collect(Collectors.toList())
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SensorData that = (SensorData) o;
        return id == that.id && timestamp.equals(that.timestamp) && sensordatavalues.equals(that.sensordatavalues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, timestamp, sensordatavalues);
    }
}
