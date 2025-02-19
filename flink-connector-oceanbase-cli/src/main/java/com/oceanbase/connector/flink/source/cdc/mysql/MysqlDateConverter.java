/*
 * Copyright 2024 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.oceanbase.connector.flink.source.cdc.mysql;

import com.oceanbase.connector.flink.config.CliConfig;

import org.apache.flink.cdc.connectors.shaded.org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.function.Consumer;

public class MysqlDateConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {
    private static final Logger LOG = LoggerFactory.getLogger(MysqlDateConverter.class);

    private DateTimeFormatter dateFormatter = DateTimeFormatter.ISO_DATE;
    private DateTimeFormatter timeFormatter = DateTimeFormatter.ISO_TIME;
    private DateTimeFormatter datetimeFormatter = DateTimeFormatter.ISO_DATE_TIME;
    private DateTimeFormatter timestampFormatter = DateTimeFormatter.ISO_DATE_TIME;
    private ZoneId timestampZoneId = ZoneId.systemDefault();

    public static final Properties DEFAULT_PROPS = new Properties();

    static {
        DEFAULT_PROPS.setProperty(CliConfig.CONVERTERS, CliConfig.DATE);
        DEFAULT_PROPS.setProperty(CliConfig.DATE_TYPE, MysqlDateConverter.class.getName());
        DEFAULT_PROPS.setProperty(CliConfig.DATE_FORMAT_DATE, CliConfig.YEAR_MONTH_DAY_FORMAT);
        DEFAULT_PROPS.setProperty(CliConfig.DATE_FORMAT_DATETIME, CliConfig.DATETIME_MICRO_FORMAT);
        DEFAULT_PROPS.setProperty(CliConfig.DATE_FORMAT_TIMESTAMP, CliConfig.DATETIME_MICRO_FORMAT);
        DEFAULT_PROPS.setProperty(CliConfig.DATE_FORMAT_TIMESTAMP_ZONE, CliConfig.TIME_ZONE_UTC_8);
    }

    @Override
    public void configure(Properties props) {
        readProps(
                props, CliConfig.FORMAT_DATE, p -> dateFormatter = DateTimeFormatter.ofPattern(p));
        readProps(
                props, CliConfig.FORMAT_TIME, p -> timeFormatter = DateTimeFormatter.ofPattern(p));
        readProps(
                props,
                CliConfig.FORMAT_DATETIME,
                p -> datetimeFormatter = DateTimeFormatter.ofPattern(p));
        readProps(
                props,
                CliConfig.FORMAT_TIMESTAMP,
                p -> timestampFormatter = DateTimeFormatter.ofPattern(p));
        readProps(props, CliConfig.FORMAT_TIMESTAMP_ZONE, z -> timestampZoneId = ZoneId.of(z));
    }

    private void readProps(Properties properties, String settingKey, Consumer<String> consumer) {
        String settingValue = (String) properties.get(settingKey);
        if (settingValue == null || settingValue.isEmpty()) {
            return;
        }
        try {
            consumer.accept(settingValue.trim());
        } catch (IllegalArgumentException | DateTimeException e) {
            LOG.error("setting {} is illegal: {}", settingKey, settingValue);
            throw e;
        }
    }

    @Override
    public void converterFor(
            RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        String sqlType = column.typeName().toUpperCase();
        SchemaBuilder schemaBuilder = null;
        Converter converter = null;
        if (CliConfig.UPPERCASE_DATE.equals(sqlType)) {
            schemaBuilder = SchemaBuilder.string().optional();
            converter = this::convertDate;
        }
        if (CliConfig.TIME.equals(sqlType)) {
            schemaBuilder = SchemaBuilder.string().optional();
            converter = this::convertTime;
        }
        if (CliConfig.DATETIME.equals(sqlType)) {
            schemaBuilder = SchemaBuilder.string().optional();
            converter = this::convertDateTime;
        }
        if (CliConfig.TIMESTAMP.equals(sqlType)) {
            schemaBuilder = SchemaBuilder.string().optional();
            converter = this::convertTimestamp;
        }
        if (schemaBuilder != null) {
            registration.register(schemaBuilder, converter);
        }
    }

    private String convertDate(Object input) {
        if (input instanceof LocalDate) {
            return dateFormatter.format((LocalDate) input);
        } else if (input instanceof Integer) {
            LocalDate date = LocalDate.ofEpochDay((Integer) input);
            return dateFormatter.format(date);
        }
        return null;
    }

    private String convertTime(Object input) {
        if (input instanceof Duration) {
            Duration duration = (Duration) input;
            long seconds = duration.getSeconds();
            int nano = duration.getNano();
            LocalTime time = LocalTime.ofSecondOfDay(seconds).withNano(nano);
            return timeFormatter.format(time);
        }
        return null;
    }

    private String convertDateTime(Object input) {
        if (input instanceof LocalDateTime) {
            return datetimeFormatter.format((LocalDateTime) input);
        } else if (input instanceof Timestamp) {
            return datetimeFormatter.format(((Timestamp) input).toLocalDateTime());
        }
        return null;
    }

    private String convertTimestamp(Object input) {
        if (input instanceof ZonedDateTime) {
            // mysql timestamp will be converted to UTC storage,
            // and the zonedDatetime here is UTC time
            ZonedDateTime zonedDateTime = (ZonedDateTime) input;
            LocalDateTime localDateTime =
                    zonedDateTime.withZoneSameInstant(timestampZoneId).toLocalDateTime();
            return timestampFormatter.format(localDateTime);
        } else if (input instanceof Timestamp) {
            return timestampFormatter.format(
                    ((Timestamp) input).toInstant().atZone(timestampZoneId).toLocalDateTime());
        }
        return null;
    }
}
