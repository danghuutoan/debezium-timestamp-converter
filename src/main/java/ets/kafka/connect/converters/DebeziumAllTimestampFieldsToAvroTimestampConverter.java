package ets.kafka.connect.converters;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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
    private List<TimestampConverter<SourceRecord>> converters = new ArrayList<>();
    private String alternativeDefaultValue;
    private List<String> columnTypes = new ArrayList<>();
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
        alternativeDefaultValue = props.getProperty("alternative.default.value", "1970-01-01 00:00:01");

        for (String format : inputFormats) {
            LOGGER.info("configure DebeziumAllTimestampFieldsToAvroTimestampConverter using format {}", format);
            Map<String, String> config = new HashMap<>();
            config.put(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp");
            config.put(TimestampConverter.FORMAT_CONFIG, format);
            TimestampConverter<SourceRecord> timestampConverter = new TimestampConverter.Value<>();
            timestampConverter.configure(config);
            converters.add(timestampConverter);
        }

    }

    @Override
    public void converterFor(RelationalColumn column,
            ConverterRegistration<SchemaBuilder> registration) {
        SchemaBuilder schema = Timestamp.builder();
        if (columnTypes.contains(column.typeName())) {

            registration.register(schema, value -> {

                value = (value != null) ? value.toString() : alternativeDefaultValue;

                SourceRecord record = new SourceRecord(null, null, null, 0, SchemaBuilder.string().schema(),
                        value);

                SourceRecord convertedRecord = null;
                Exception exception = null;
                for (TimestampConverter<SourceRecord> converter : converters) {
                    try {
                        convertedRecord = converter.apply(record);
                        break;
                    } catch (DataException e) {
                        exception = e;
                        LOGGER.warn(e.getMessage());
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
                return convertedRecord.value();
            });
        }
    }
}