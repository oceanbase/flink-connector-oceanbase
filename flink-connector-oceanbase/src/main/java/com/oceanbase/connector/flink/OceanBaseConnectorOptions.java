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

import com.alipay.oceanbase.rpc.protocol.payload.impl.direct_load.ObLoadDupActionType;

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

    public static final ConfigOption<Boolean> DIRECT_LOAD_ENABLED =
            ConfigOptions.key("direct-load.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to enable direct load.");

    public static final ConfigOption<String> DIRECT_LOAD_HOST =
            ConfigOptions.key("direct-load.host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Hostname used in direct load.");

    public static final ConfigOption<Integer> DIRECT_LOAD_PORT =
            ConfigOptions.key("direct-load.port")
                    .intType()
                    .defaultValue(2882)
                    .withDescription("Rpc port number used in direct load.");

    public static final ConfigOption<Integer> DIRECT_LOAD_PARALLEL =
            ConfigOptions.key("direct-load.parallel")
                    .intType()
                    .defaultValue(8)
                    .withDescription("Parallelism of direct load.");

    public static final ConfigOption<Long> DIRECT_LOAD_MAX_ERROR_ROWS =
            ConfigOptions.key("direct-load.max-error-rows")
                    .longType()
                    .defaultValue(0L)
                    .withDescription("Maximum tolerable number of error rows.");

    public static final ConfigOption<ObLoadDupActionType> DIRECT_LOAD_DUP_ACTION =
            ConfigOptions.key("direct-load.dup-action")
                    .enumType(ObLoadDupActionType.class)
                    .defaultValue(ObLoadDupActionType.REPLACE)
                    .withDescription("Action when there is duplicated record in direct load.");

    public static final ConfigOption<Duration> DIRECT_LOAD_TIMEOUT =
            ConfigOptions.key("direct-load.timeout")
                    .durationType()
                    .defaultValue(Duration.ofDays(7))
                    .withDescription("Timeout for direct load task.");

    public static final ConfigOption<Duration> DIRECT_LOAD_HEARTBEAT_TIMEOUT =
            ConfigOptions.key("direct-load.heartbeat-timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription("Client heartbeat timeout in direct load task.");

    public static final ConfigOption<Boolean> COLUMN_NAME_CASE_INSENSITIVE =
            ConfigOptions.key("column.name.case.insensitive")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("By default, column name matching is case-insensitive.");

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

    public boolean getDirectLoadEnabled() {
        return allConfig.get(DIRECT_LOAD_ENABLED);
    }

    public String getDirectLoadHost() {
        return allConfig.get(DIRECT_LOAD_HOST);
    }

    public int getDirectLoadPort() {
        return allConfig.get(DIRECT_LOAD_PORT);
    }

    public int getDirectLoadParallel() {
        return allConfig.get(DIRECT_LOAD_PARALLEL);
    }

    public long getDirectLoadMaxErrorRows() {
        return allConfig.get(DIRECT_LOAD_MAX_ERROR_ROWS);
    }

    public ObLoadDupActionType getDirectLoadDupAction() {
        return allConfig.get(DIRECT_LOAD_DUP_ACTION);
    }

    public long getDirectLoadTimeout() {
        return allConfig.get(DIRECT_LOAD_TIMEOUT).toNanos() / 1000;
    }

    public long getDirectLoadHeartbeatTimeout() {
        return allConfig.get(DIRECT_LOAD_HEARTBEAT_TIMEOUT).toNanos() / 1000;
    }

    public boolean getColumnNameCaseInsensitive() {
        return allConfig.get(COLUMN_NAME_CASE_INSENSITIVE);
    }
}
