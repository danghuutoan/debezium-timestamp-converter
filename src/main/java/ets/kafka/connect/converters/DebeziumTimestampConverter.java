package ets.kafka.connect.converters;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.TimestampConverter;

public class DebeziumTimestampConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {
    private final TimestampConverter<SourceRecord> timestampConverter = new TimestampConverter.Value<>();

    @Override
    public void configure(Properties props) {
        Map<String, String> config = new HashMap<>();
        config.put(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp");
        config.put(TimestampConverter.FORMAT_CONFIG, props.getProperty(TimestampConverter.FORMAT_CONFIG));
        timestampConverter.configure(config);
    }

    @Override
    public void converterFor(RelationalColumn column,
            ConverterRegistration<SchemaBuilder> registration) {

        if ("TIMESTAMP".equals(column.typeName())) {
            registration.register(Timestamp.builder(), x -> {

                SourceRecord record = new SourceRecord(null, null, null, 0, SchemaBuilder.string().schema(), x.toString());
                record = timestampConverter.apply(record);
                return record.value();
            });
        }
    }
}
