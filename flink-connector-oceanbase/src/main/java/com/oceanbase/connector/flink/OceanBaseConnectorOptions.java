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

import com.oceanbase.connector.flink.connection.OceanBaseConnectionOptions;
import com.oceanbase.connector.flink.sink.OceanBaseStatementOptions;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.time.Duration;
import java.util.Map;

public class OceanBaseConnectorOptions extends AbstractOceanBaseConnectorOptions {
    private static final long serialVersionUID = 1L;

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

    public static final ConfigOption<String> COMPATIBLE_MODE =
            ConfigOptions.key("compatible-mode")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The compatible mode of OceanBase, can be 'mysql' or 'oracle'.");

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
                    .withDescription(
                            "Whether to enable partition calculation and flush records by partitions.");

    public static final ConfigOption<Integer> PARTITION_NUMBER =
            ConfigOptions.key("partition.number")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "The number of partitions. When the 'partition.enabled' is 'true', the same number of threads will be used to flush records in parallel.");

    public OceanBaseConnectorOptions(Map<String, String> config) {
        super(config);
    }

    @Override
    protected void validateConfig() {
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
                parseProperties(allConfig.get(CONNECTION_POOL_PROPERTIES)));
    }

    public OceanBaseStatementOptions getStatementOptions() {
        return new OceanBaseStatementOptions(
                allConfig.get(UPSERT_MODE),
                allConfig.get(BUFFER_BATCH_SIZE),
                allConfig.get(MEMSTORE_CHECK_ENABLED),
                allConfig.get(MEMSTORE_THRESHOLD),
                allConfig.get(MEMSTORE_CHECK_INTERVAL).toMillis(),
                allConfig.get(PARTITION_ENABLED),
                allConfig.get(PARTITION_NUMBER));
    }
}
