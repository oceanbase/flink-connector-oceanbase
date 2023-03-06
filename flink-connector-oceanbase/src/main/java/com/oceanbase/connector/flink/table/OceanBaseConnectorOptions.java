/*
 * Copyright (c) 2023 OceanBase
 * flink-connector-oceanbase is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *         http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package com.oceanbase.connector.flink.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;

import com.oceanbase.connector.flink.connection.OceanBaseConnectionOptions;
import com.oceanbase.connector.flink.sink.OceanBaseWriterOptions;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;

public class OceanBaseConnectorOptions implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final ConfigOption<String> URL =
            ConfigOptions.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The JDBC database URL.");

    public static final ConfigOption<String> TABLE_NAME =
            ConfigOptions.key("table-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The table name.");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The username.");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The password.");

    public static final ConfigOption<String> DRIVER_CLASS_NAME =
            ConfigOptions.key("driver-class")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("JDBC driver class name.");

    public static final ConfigOption<String> CONNECTION_POOL =
            ConfigOptions.key("connection-pool")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Connection pool type, can be 'druid' or 'hikari'.");

    public static final ConfigOption<String> CONNECTION_POOL_PROPERTIES =
            ConfigOptions.key("connection-pool-properties")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Properties for specific connection pool.");

    public static final ConfigOption<Boolean> UPSERT_MODE =
            ConfigOptions.key("upsert-mode")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Flag of whether to use upsert mode.");

    public static final ConfigOption<Duration> BUFFER_FLUSH_INTERVAL =
            ConfigOptions.key("buffer-flush.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription(
                            "The flush interval mills, over this time, asynchronous threads will flush data.");

    public static final ConfigOption<Integer> BUFFER_SIZE =
            ConfigOptions.key("buffer-flush.buffer-size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("The flush batch size of records buffer.");

    public static final ConfigOption<Integer> BUFFER_BATCH_SIZE =
            ConfigOptions.key("buffer-flush.batch-size")
                    .intType()
                    .defaultValue(100)
                    .withDescription("The flush batch size of records buffer.");

    public static final ConfigOption<Integer> MAX_RETRIES =
            ConfigOptions.key("max-retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription("The max retry times if writing records to database failed.");
    private final ReadableConfig allConfig;

    public OceanBaseConnectorOptions(Map<String, String> allOptions) {
        this.allConfig = Configuration.fromMap(allOptions);
    }

    public OceanBaseConnectionOptions getConnectionOptions() {
        return new OceanBaseConnectionOptions(
                allConfig.get(URL),
                allConfig.get(USERNAME),
                allConfig.get(PASSWORD),
                allConfig.get(DRIVER_CLASS_NAME),
                allConfig.get(CONNECTION_POOL),
                parseProperties(allConfig.get(CONNECTION_POOL_PROPERTIES)));
    }

    public OceanBaseWriterOptions getWriterOptions() {
        return new OceanBaseWriterOptions(
                allConfig.get(TABLE_NAME),
                allConfig.get(UPSERT_MODE),
                allConfig.get(BUFFER_FLUSH_INTERVAL).toMillis(),
                allConfig.get(BUFFER_SIZE).intValue(),
                allConfig.get(BUFFER_BATCH_SIZE).intValue(),
                allConfig.get(MAX_RETRIES).intValue());
    }

    private Properties parseProperties(String propsStr) {
        Properties props = new Properties();
        if (StringUtils.isBlank(propsStr)) {
            return props;
        }
        for (String propStr : propsStr.split(";")) {
            if (StringUtils.isBlank(propStr)) {
                continue;
            }
            String[] pair = propStr.trim().split("=");
            if (pair.length != 2) {
                throw new IllegalArgumentException("properties must have one key value pair");
            }
            props.put(pair[0].trim(), pair[1].trim());
        }
        return props;
    }
}
