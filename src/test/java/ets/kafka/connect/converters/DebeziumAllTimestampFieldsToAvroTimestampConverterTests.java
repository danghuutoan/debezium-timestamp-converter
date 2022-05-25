package ets.kafka.connect.converters;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;
import org.junit.Test;
import org.junit.Before;
import org.fest.assertions.Assertions;
import io.debezium.spi.converter.RelationalColumn;
import io.debezium.spi.converter.CustomConverter.Converter;
import io.debezium.spi.converter.CustomConverter.ConverterRegistration;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.TimeZone;

public class DebeziumAllTimestampFieldsToAvroTimestampConverterTests {

    public static class BasicColumn implements RelationalColumn {

        private final String name;
        private final String dataCollection;
        private final String typeName;


        public BasicColumn(String name, String dataCollection, String typeName) {
            super();
            this.name = name;
            this.dataCollection = dataCollection;
            this.typeName = typeName;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public String dataCollection() {
            return dataCollection;
        }

        @Override
        public String typeName() {
            return typeName;
        }

        @Override
        public int jdbcType() {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public int nativeType() {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public String typeExpression() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public OptionalInt length() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public OptionalInt scale() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public boolean isOptional() {
            // TODO Auto-generated method stub
            return false;
        }

        @Override
        public Object defaultValue() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public boolean hasDefaultValue() {
            // TODO Auto-generated method stub
            return false;
        }
    }

    private static class TestRegistration implements ConverterRegistration<SchemaBuilder> {
        public SchemaBuilder fieldSchema;
        public Converter converter;

        @Override
        public void register(SchemaBuilder fieldSchema, Converter converter) {
            this.fieldSchema = fieldSchema;
            this.converter = converter;
        }
    }

    private TestRegistration testRegistration;

    @Before
    public void before() {
        testRegistration = new TestRegistration();
    }

    @Test
    public void testShouldHandleTimestampType() throws ParseException {
        final String input = "2022-05-20T00:35:29Z";
        final DebeziumAllTimestampFieldsToAvroTimestampConverter tsConverter = new DebeziumAllTimestampFieldsToAvroTimestampConverter();
        final String format = "yyyy-MM-dd'T'HH:mm:ss'Z'";
        Properties props = new Properties();

        props.put("debug", "true");
        props.put("input.formats", "yyyy-MM-dd HH:mm:ss.S;yyyy-MM-dd'T'HH:mm:ss'Z'");
        tsConverter.configure(props);
        tsConverter.converterFor(new BasicColumn("myfield", "db1.table1", "TIMESTAMP"), testRegistration);
        Date actualResult = (Date) testRegistration.converter.convert(input);
        Date expectedResult = getFormater(format).parse(input);
        Assertions.assertThat(testRegistration.fieldSchema.name()).isEqualTo("org.apache.kafka.connect.data.Timestamp");
        Assertions.assertThat(actualResult.equals(expectedResult)).isEqualTo(true);
    }


    @Test
    public void testShouldIgoreStringType() throws ParseException {
        final DebeziumAllTimestampFieldsToAvroTimestampConverter tsConverter = new DebeziumAllTimestampFieldsToAvroTimestampConverter();
        final String format = "yyyy-MM-dd'T'HH:mm:ss'Z'";
        Properties props = new Properties();

        props.put("input.formats", "yyyy-MM-dd HH:mm:ss.S;yyyy-MM-dd'T'HH:mm:ss'Z'");
        props.put("debug", "true");
        props.put("format", format);
        tsConverter.configure(props);
        tsConverter.converterFor(new BasicColumn("myfield", "db1.table1", "String"), testRegistration);
        Assertions.assertThat(testRegistration.converter ==null).isEqualTo(true);
    }

    @Test(expected = DataException.class)
    public void testInvalidDatetimeFormat(){
        final String input = "2022-05-20T00:35:29Z";
        final DebeziumAllTimestampFieldsToAvroTimestampConverter tsConverter = new DebeziumAllTimestampFieldsToAvroTimestampConverter();
        final String format = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
        Properties props = new Properties();
        props.put("format", format);
        props.put("debug", "true");
        props.put("input.formats", "yyyy-MM-dd HH:mm:ss.S;yyyy-MM-dd'T'HH:mm:ss.SS'Z'");
        tsConverter.configure(props);
        tsConverter.converterFor(new BasicColumn("myfield", "db1.table1", "TIMESTAMP"), testRegistration);
        testRegistration.converter.convert(input);
    }

    @Test
    public void shouldHandleMultiDatetimeFormat(){
        final String input = "2022-05-20T00:35:29Z";
        final DebeziumAllTimestampFieldsToAvroTimestampConverter tsConverter = new DebeziumAllTimestampFieldsToAvroTimestampConverter();
        final String format = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
        Properties props = new Properties();

        props.put("input.formats", "yyyy-MM-dd HH:mm:ss.S;yyyy-MM-dd'T'HH:mm:ss'Z'");
        props.put("format", format);
        props.put("debug", "true");
        tsConverter.configure(props);
        tsConverter.converterFor(new BasicColumn("myfield", "db1.table1", "TIMESTAMP"), testRegistration);
        testRegistration.converter.convert(input);
    }

    SimpleDateFormat getFormater(String parttern) {
        SimpleDateFormat simpleDatetimeFormatter = new SimpleDateFormat(parttern);
        simpleDatetimeFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        return simpleDatetimeFormatter;
    }
}