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
    private static final Logger LOGGER = LoggerFactory
            .getLogger(DebeziumAllTimestampFieldsToAvroTimestampConverter.class);

    @Override
    public void configure(Properties props) {

        Iterable<String> inputFormats = null;
        try {
            inputFormats = Arrays.asList(props.getProperty("input.formats").split(";"));
        } catch (NullPointerException e) {
            throw new ConfigException(
                    "DebeziumAllTimestampFieldsToAvroTimestampConverter requires a SimpleDateFormat-compatible pattern for string timestamps");
        }

        for (String format : inputFormats) {
            System.out.println(format);
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

        if ("TIMESTAMP".equals(column.typeName())) {
            registration.register(Timestamp.builder(), value -> {

                SourceRecord record = new SourceRecord(null, null, null, 0, SchemaBuilder.string().schema(),
                        value.toString());

                LOGGER.debug("received {}", value.toString());
                SourceRecord convertedRecord = null;
                Exception exception = null;
                for (TimestampConverter<SourceRecord> converter : converters) {
                    try {
                        convertedRecord = converter.apply(record);
                    } catch (DataException e) {
                        exception = e;
                        LOGGER.warn("Failed to parse datetime using converter {}", converter.toString());
                    }
                }

                if (convertedRecord == null) {
                    if (exception == null) {
                        throw new RuntimeException(
                                "Bug Alert TimestampConverter: if record is null, exception should be provided");
                    }
                    throw new DataException(exception);
                }
                return convertedRecord.value();
            });
        }
    }

}
