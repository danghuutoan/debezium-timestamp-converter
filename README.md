![](https://github.com/oryanmoshe/debezium-timestamp-converter/workflows/Run%20Tests/badge.svg?branch=master) ![](https://github.com/oryanmoshe/debezium-timestamp-converter/workflows/GitHub%20Release/badge.svg) ![](https://github.com/oryanmoshe/debezium-timestamp-converter/workflows/GitHub%20Package/badge.svg)
# Debezium Timestamp Converter
This is a custom converter to use with debezium (using their SPI, introduced in version 1.1)  
You can use it to convert all temporal data types (in all databases) into a specified format you choose.

## Usage
You can either download the `.jar` file from the [releases](https://github.com/danghuutoan/debezium-timestamp-converter/releases) and include it in your connector's folder, or add the converter as a dependency to your maven project.

**You have to add this converter to each of your connectors, not just in the main folder (`/kafka/connect`)!**

## Configuration
### Basic Configuration
To configure this converter all you need to do is add the following lines to your connector configuration:
```json
"converters": "timestampConverter",
"timestampConverter.type": "ets.kafka.connect.converters.DebeziumAllTimestampFieldsToAvroTimestampConverter",
"timestampConverter.input.formats": "yyyy-MM-dd'T'HH:mm'Z';yyyy-MM-dd HH:mm:ss;yyyy-MM-dd'T'HH:mm:ss'Z'",
"timestampConverter.alternative.default.value": "null",
"timestampConverter.null.equivalent.values": "1970-01-01T00:00Z;0000-00-00 00:00:00;1970-01-01T00:00:00Z",
"timestampConverter.debug": "true",
```
# debezium-timestamp-converter
