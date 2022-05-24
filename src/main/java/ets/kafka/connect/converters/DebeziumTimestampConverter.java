package ets.kafka.connect.converters;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.TimestampConverter;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DebeziumTimestampConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {
    private List<TimestampConverter<SourceRecord>> converters = new ArrayList<>();
    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumTimestampConverter.class);

    @Override
    public void configure(Properties props) {

        Iterable<String> inputFormats = Arrays.asList(props.getProperty("input.formats").split(";"));

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
            registration.register(Timestamp.builder(), x -> {

                SourceRecord record = new SourceRecord(null, null, null, 0, SchemaBuilder.string().schema(),
                        x.toString());
                
                LOGGER.warn("received {}", x.toString());
                System.out.printf("received %s", x.toString());
                for (TimestampConverter<SourceRecord> converter : converters) {
                    try {
                        record = converter.apply(record);
                        break;
                    } catch (DataException e) {
                        // TODO: handle exception
                        int index = converters.indexOf(converter);
                        System.out.println(index);
                        if (index == converters.size() - 1) {
                            // LOGGER.warn("Failed to parse datetime using all converters");
                            throw new DataException(e);
                        } else {
                            // LOGGER.warn("Failed to parse datetime using format using converter {}, try the next one", index);
                        }

                    }
                }
                return record.value();
            });
        }
    }
}
