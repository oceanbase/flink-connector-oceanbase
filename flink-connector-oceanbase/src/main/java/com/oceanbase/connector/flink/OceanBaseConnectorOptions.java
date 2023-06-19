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

package com.oceanbase.connector.flink;

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

    public static final ConfigOption<String> CLUSTER_NAME =
            ConfigOptions.key("cluster-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The cluster name.");

    public static final ConfigOption<String> TENANT_NAME =
            ConfigOptions.key("tenant-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The tenant name.");

    public static final ConfigOption<String> SCHEMA_NAME =
            ConfigOptions.key("schema-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The schema name.");

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

    public static final ConfigOption<String> COMPATIBLE_MODE =
            ConfigOptions.key("compatible-mode")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The compatible mode of OceanBase, can be 'mysql' or 'oracle'.");

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

    public static final ConfigOption<Boolean> MEMSTORE_CHECK_ENABLED =
            ConfigOptions.key("memstore-check.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether enable memstore check.");

    public static final ConfigOption<Double> MEMSTORE_THRESHOLD =
            ConfigOptions.key("memstore-check.threshold")
                    .doubleType()
                    .defaultValue(0.9)
                    .withDescription("Memory usage threshold ratio relative to the limit value.");

    public static final ConfigOption<Duration> MEMSTORE_CHECK_INTERVAL =
            ConfigOptions.key("memstore-check.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription(
                            "The check interval mills, over this time, the writer will check if memstore reaches threshold.");

    public static final ConfigOption<Boolean> PARTITION_ENABLED =
            ConfigOptions.key("partition.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether enable partition calculation.");

    private final ReadableConfig allConfig;

    public OceanBaseConnectorOptions(Map<String, String> allOptions) {
        this.allConfig = Configuration.fromMap(allOptions);
        if (allConfig.get(PARTITION_ENABLED)
                && (allConfig.get(CLUSTER_NAME) == null || allConfig.get(TENANT_NAME) == null)) {
            throw new IllegalArgumentException(
                    "'cluster-name' and 'tenant-name' are required when 'partition.enabled' is true.");
        }
    }

    public OceanBaseConnectionOptions getConnectionOptions() {
        return new OceanBaseConnectionOptions(
                allConfig.get(URL),
                allConfig.get(CLUSTER_NAME),
                allConfig.get(TENANT_NAME),
                allConfig.get(SCHEMA_NAME),
                allConfig.get(TABLE_NAME),
                allConfig.get(USERNAME),
                allConfig.get(PASSWORD),
                allConfig.get(COMPATIBLE_MODE),
                allConfig.get(DRIVER_CLASS_NAME),
                allConfig.get(CONNECTION_POOL),
                parseProperties(allConfig.get(CONNECTION_POOL_PROPERTIES)));
    }

    public OceanBaseWriterOptions getWriterOptions() {
        return new OceanBaseWriterOptions(
                allConfig.get(UPSERT_MODE),
                allConfig.get(BUFFER_FLUSH_INTERVAL).toMillis(),
                allConfig.get(BUFFER_SIZE),
                allConfig.get(BUFFER_BATCH_SIZE),
                allConfig.get(MAX_RETRIES),
                allConfig.get(MEMSTORE_CHECK_ENABLED),
                allConfig.get(MEMSTORE_THRESHOLD),
                allConfig.get(MEMSTORE_CHECK_INTERVAL).toMillis(),
                allConfig.get(PARTITION_ENABLED));
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
