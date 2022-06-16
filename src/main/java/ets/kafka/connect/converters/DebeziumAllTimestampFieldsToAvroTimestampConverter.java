package ets.kafka.connect.converters;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.TimestampConverter;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DebeziumAllTimestampFieldsToAvroTimestampConverter
        implements CustomConverter<SchemaBuilder, RelationalColumn> {
    public static final String UNIX_START_TIME = "1970-01-01 00:00:00";
    public static final String MYSQL_ZERO_DATETIME = "0000-00-00 00:00:00";
    private List<TimestampConverter<SourceRecord>> converters = new ArrayList<>();
    private Boolean debug;
    private List<String> columnTypes = new ArrayList<>();
    private final Map<String, TimestampConverter<SourceRecord>> converterMap = new LinkedHashMap<>();
    private static final Logger LOGGER = LoggerFactory
            .getLogger(DebeziumAllTimestampFieldsToAvroTimestampConverter.class);

    @Override
    public void configure(Properties props) {

        Iterable<String> inputFormats = null;
        try {
            inputFormats = Arrays.asList(props.getProperty("input.formats").split(";"));
        } catch (NullPointerException e) {
            throw new ConfigException(
                    "No input datetime format provided");
        }
        columnTypes = Arrays.asList(props.getProperty("column.types", "TIMESTAMP").split(";"));
        debug = props.getProperty("debug", "false").equals("true");
    
        for (String format : inputFormats) {
            LOGGER.info("configure DebeziumAllTimestampFieldsToAvroTimestampConverter using format {}", format);
            Map<String, String> config = new HashMap<>();
            config.put(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp");
            config.put(TimestampConverter.FORMAT_CONFIG, format);
            TimestampConverter<SourceRecord> timestampConverter = new TimestampConverter.Value<>();
            timestampConverter.configure(config);
            converters.add(timestampConverter);
        }

        Map<String, String> config = new HashMap<>();
        config.put(TimestampConverter.TARGET_TYPE_CONFIG, "Date");
        TimestampConverter<SourceRecord> timestampConverter = new TimestampConverter.Value<>();
        timestampConverter.configure(config);
        converterMap.put("Date", timestampConverter);

        config = new HashMap<>();
        config.put(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp");
        timestampConverter = new TimestampConverter.Value<>();
        timestampConverter.configure(config);
        converterMap.put("Timestamp", timestampConverter);
    }

    @Override
    public void converterFor(RelationalColumn column,
            ConverterRegistration<SchemaBuilder> registration) {

        if (column.typeName().equals("TIMESTAMP") || columnTypes.contains(column.typeName())) {
            SchemaBuilder schema = Timestamp.builder().optional().defaultValue(null);
            registration.register(schema, value -> convertTimestamp(value));

        } else if (column.typeName().equals("DATE")) {
            SchemaBuilder schema = Date.builder().optional().defaultValue(null);
            registration.register(schema, value -> convertDate(value));

        } else if (column.typeName().equals("DATETIME")) {
            SchemaBuilder schema = Timestamp.builder().optional().defaultValue(null);
            registration.register(schema, value -> convertDatetime(value));

        } else {
            if (debug)
                LOGGER.info("##########{}", column.typeName());
        }
    }

    private Object convertTimestamp(Object value) {
        if (debug)
            LOGGER.info("Received value{}", value);

        if (value == null)
            return null;

        SourceRecord record = new SourceRecord(null, null, null, 0, SchemaBuilder.string().schema(),
                value.toString());

        SourceRecord convertedRecord = null;
        Exception exception = null;
        for (TimestampConverter<SourceRecord> converter : converters) {
            try {
                convertedRecord = converter.apply(record);
                break;
            } catch (DataException e) {
                exception = e;
            }
        }

        if (convertedRecord == null) {
            if (exception == null) {
                throw new RuntimeException(
                        "Bug Alert TimestampConverter: if record is null, exception should be provided");
            }
            LOGGER.error("Provided input format are not compatible with data.");
            throw new DataException(exception);
        }
        java.util.Date result = (java.util.Date) convertedRecord.value();
        if (result != null && result.getTime() <= 0)
            return null;
        return result;
    }

    private Object convertDate(Object value) {

        TimestampConverter<SourceRecord> timestampConverter = converterMap.get("Date");
        if (value instanceof LocalDate)
            value = java.sql.Date.valueOf((LocalDate) value);

        SourceRecord record = new SourceRecord(null, null, null, 0,
                Date.builder().optional().defaultValue(null).schema(),
                value);

        java.util.Date result = (java.util.Date) timestampConverter.apply(record).value();
        if (result != null && result.getTime() <= 0)
            return null;
        return result;

    }

    private Object convertDatetime(Object value) {
        TimestampConverter<SourceRecord> timestampConverter = converterMap.get("Timestamp");
        if (value instanceof LocalDateTime)
            value = java.util.Date.from(((LocalDateTime) value).atZone(ZoneId.systemDefault()).toInstant());
        SourceRecord record = new SourceRecord(null, null, null, 0,
                Timestamp.builder().optional().defaultValue(null).schema(),
                value);
        java.util.Date result = (java.util.Date) timestampConverter.apply(record).value();
        if (result != null && result.getTime() <= 0)
            return null;
        return result;

    }
}