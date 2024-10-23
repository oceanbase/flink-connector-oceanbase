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

package com.oceanbase.connector.flink;

import com.oceanbase.connector.flink.utils.OptionUtils;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;

public class OceanBaseConnectorOptions extends ConnectorOptions {
    private static final long serialVersionUID = 1L;

    public static final ConfigOption<String> DRIVER_CLASS_NAME =
            ConfigOptions.key("driver-class-name")
                    .stringType()
                    .defaultValue("com.mysql.cj.jdbc.Driver")
                    .withDescription(
                            "JDBC driver class name, use 'com.mysql.cj.jdbc.Driver' by default.");

    public static final ConfigOption<String> DRUID_PROPERTIES =
            ConfigOptions.key("druid-properties")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Properties for specific connection pool.");

    public static final ConfigOption<Boolean> MEMSTORE_CHECK_ENABLED =
            ConfigOptions.key("memstore-check.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether enable memstore check. Default value is 'true'");

    public static final ConfigOption<Double> MEMSTORE_THRESHOLD =
            ConfigOptions.key("memstore-check.threshold")
                    .doubleType()
                    .defaultValue(0.9)
                    .withDescription(
                            "Memory usage threshold ratio relative to the limit value. Default value is '0.9'.");

    public static final ConfigOption<Duration> MEMSTORE_CHECK_INTERVAL =
            ConfigOptions.key("memstore-check.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription(
                            "The check interval, over this time, the writer will check if memstore reaches threshold. Default value is '30s'.");

    public static final ConfigOption<Boolean> PARTITION_ENABLED =
            ConfigOptions.key("partition.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to enable partition calculation and flush records by partitions. Default value is 'false'.");

    public static final ConfigOption<Boolean> TABLE_ORACLE_TENANT_CASE_INSENSITIVE =
            ConfigOptions.key("table.oracle-tenant-case-insensitive")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "By default, under the Oracle tenant, schema names and column names are case-insensitive.");

    public OceanBaseConnectorOptions(Map<String, String> config) {
        super(config);
    }

    public String getDriverClassName() {
        return allConfig.get(DRIVER_CLASS_NAME);
    }

    public Properties getDruidProperties() {
        return OptionUtils.parseProperties(allConfig.get(DRUID_PROPERTIES));
    }

    public boolean getMemStoreCheckEnabled() {
        return allConfig.get(MEMSTORE_CHECK_ENABLED);
    }

    public double getMemStoreThreshold() {
        return allConfig.get(MEMSTORE_THRESHOLD);
    }

    public long getMemStoreCheckInterval() {
        return allConfig.get(MEMSTORE_CHECK_INTERVAL).toMillis();
    }

    public boolean getPartitionEnabled() {
        return allConfig.get(PARTITION_ENABLED);
    }

    public boolean getTableOracleTenantCaseInsensitive() {
        return allConfig.get(TABLE_ORACLE_TENANT_CASE_INSENSITIVE);
    }
}
