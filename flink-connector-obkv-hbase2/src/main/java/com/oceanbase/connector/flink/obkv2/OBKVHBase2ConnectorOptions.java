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

package com.oceanbase.connector.flink.obkv2;

import com.oceanbase.connector.flink.ConnectorOptions;
import com.oceanbase.connector.flink.utils.OptionUtils;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/** Unified configuration options for OBKV HBase2 connector. */
public class OBKVHBase2ConnectorOptions extends ConnectorOptions {

    private static final long serialVersionUID = 1L;

    // Connection options
    public static final ConfigOption<Boolean> ODP_MODE =
            ConfigOptions.key("odp-mode")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to use ODP to connect to OBKV.");

    public static final ConfigOption<String> ODP_IP =
            ConfigOptions.key("odp-ip")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("ODP IP address. Required when 'odp-mode' is true.");

    public static final ConfigOption<Integer> ODP_PORT =
            ConfigOptions.key("odp-port")
                    .intType()
                    .defaultValue(2885)
                    .withDescription("ODP rpc port.");

    public static final ConfigOption<String> SYS_USERNAME =
            ConfigOptions.key("sys.username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The username of system tenant. Required when 'odp-mode' is false.");

    public static final ConfigOption<String> SYS_PASSWORD =
            ConfigOptions.key("sys.password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The password of system tenant. Required when 'odp-mode' is false.");

    public static final ConfigOption<String> HBASE_PROPERTIES =
            ConfigOptions.key("hbase.properties")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Properties to configure 'obkv-hbase-client-java', separated by semicolon.");

    // HBase specific options
    public static final ConfigOption<String> COLUMN_FAMILY =
            ConfigOptions.key("columnFamily")
                    .stringType()
                    .defaultValue("f")
                    .withDescription("The column family name.");

    public static final ConfigOption<String> ROWKEY_DELIMITER =
            ConfigOptions.key("rowkeyDelimiter")
                    .stringType()
                    .defaultValue(":")
                    .withDescription(
                            "The delimiter to combine multiple primary key columns into one rowkey.");

    public static final ConfigOption<Boolean> WRITE_PK_VALUE =
            ConfigOptions.key("writePkValue")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to write primary key values as column values in HBase.");

    // Sink behavior options
    public static final ConfigOption<Integer> BUFFER_SIZE =
            ConfigOptions.key("bufferSize")
                    .intType()
                    .defaultValue(5000)
                    .withDescription("The buffer size for batch writing.");

    public static final ConfigOption<Duration> FLUSH_INTERVAL =
            ConfigOptions.key("flushIntervalMs")
                    .durationType()
                    .defaultValue(Duration.ofMillis(2000))
                    .withDescription("The flush interval for batch writing.");

    public static final ConfigOption<Boolean> IGNORE_NULL =
            ConfigOptions.key("ignoreNullWhenUpdate")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to ignore null values when updating.");

    public static final ConfigOption<Boolean> IGNORE_DELETE =
            ConfigOptions.key("ignoreDelete")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to ignore delete operations.");

    public static final ConfigOption<String> EXCLUDE_UPDATE_COLUMNS =
            ConfigOptions.key("excludeUpdateColumns")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Comma-separated list of columns to exclude from update operations.");

    public static final ConfigOption<Boolean> DYNAMIC_COLUMN_SINK =
            ConfigOptions.key("dynamicColumnSink")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to enable dynamic column mode.");

    // Timestamp options
    /**
     * Timestamp column configuration
     *
     * <p>Purpose: Specifies which column to use as timestamp for HBase version time setting
     * Priority: tsColumn > tsMap > system current time
     *
     * <p>Example: If table has update_time column, set tsColumn=update_time Result: All columns use
     * update_time column's value as timestamp
     */
    public static final ConfigOption<String> TS_COLUMN =
            ConfigOptions.key("tsColumn")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The column name to use as timestamp for all columns. If set, 'tsMap' will be ignored.");

    /**
     * Timestamp mapping configuration
     *
     * <p>Purpose: Fine-grained control for different columns using different timestamp columns
     * Format: 'tsColumn0:column0;tsColumn0:column1;tsColumn1:column2'
     *
     * <p>Example: 'update_time:name;update_time:age;create_time:status' Result: - name and age
     * columns use update_time as timestamp - status column uses create_time as timestamp
     */
    public static final ConfigOption<String> TS_MAP =
            ConfigOptions.key("tsMap")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Mapping from timestamp columns to data columns in format: 'tsColumn0:column0;tsColumn0:column1;tsColumn1:column2'");

    /**
     * Timestamp unit configuration
     *
     * <p>Purpose: Specifies the unit of timestamp values Default: true (milliseconds)
     *
     * <p>true: Timestamp is in milliseconds (e.g., 1640995200000) false: Timestamp is in seconds
     * (e.g., 1640995200)
     */
    public static final ConfigOption<Boolean> TS_IN_MILLS =
            ConfigOptions.key("tsInMills")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether timestamp values are in milliseconds.");

    // Retry options
    public static final ConfigOption<Long> MAX_RETRY_TIMES =
            ConfigOptions.key("maxRetryTimes")
                    .longType()
                    .defaultValue(5L)
                    .withDescription("The maximum number of retries.");

    public static final ConfigOption<Long> RETRY_INTERVAL_MS =
            ConfigOptions.key("retryIntervalMs")
                    .longType()
                    .defaultValue(1000L)
                    .withDescription("The interval between retries in milliseconds.");

    public OBKVHBase2ConnectorOptions(Map<String, String> config) {
        super(config);
    }

    // Connection getters
    public Boolean getOdpMode() {
        return allConfig.get(ODP_MODE);
    }

    public String getOdpIp() {
        return allConfig.get(ODP_IP);
    }

    public Integer getOdpPort() {
        return allConfig.get(ODP_PORT);
    }

    public String getSysUsername() {
        return allConfig.get(SYS_USERNAME);
    }

    public String getSysPassword() {
        return allConfig.get(SYS_PASSWORD);
    }

    public Properties getHbaseProperties() {
        return OptionUtils.parseProperties(allConfig.get(HBASE_PROPERTIES));
    }

    // HBase specific getters
    public String getColumnFamily() {
        return allConfig.get(COLUMN_FAMILY);
    }

    public String getRowkeyDelimiter() {
        return allConfig.get(ROWKEY_DELIMITER);
    }

    public Boolean getWritePkValue() {
        return allConfig.get(WRITE_PK_VALUE);
    }

    // Sink behavior getters
    public int getBufferSize() {
        return allConfig.get(BUFFER_SIZE);
    }

    public Long getFlushIntervalMs() {
        return allConfig.get(FLUSH_INTERVAL).toMillis();
    }

    public Boolean getIgnoreNull() {
        return allConfig.get(IGNORE_NULL);
    }

    public Boolean getIgnoreDelete() {
        return allConfig.get(IGNORE_DELETE);
    }

    public String getExcludeUpdateColumns() {
        return allConfig.get(EXCLUDE_UPDATE_COLUMNS);
    }

    public Boolean getDynamicColumnSink() {
        return allConfig.get(DYNAMIC_COLUMN_SINK);
    }

    // Timestamp getters
    public String getTsColumn() {
        return allConfig.get(TS_COLUMN);
    }

    public String getTsMap() {
        return allConfig.get(TS_MAP);
    }

    public Boolean getTsInMills() {
        return allConfig.get(TS_IN_MILLS);
    }

    // Retry getters
    public Long getMaxRetryTimes() {
        return allConfig.get(MAX_RETRY_TIMES);
    }

    public Long getRetryIntervalMs() {
        return allConfig.get(RETRY_INTERVAL_MS);
    }

    // Additional fields for SinkConfig compatibility
    private Integer tsColumn;
    private Map<Integer, List<Integer>> tsMap;

    public void setTsColumn(Integer tsColumn) {
        this.tsColumn = tsColumn;
    }

    public Integer getTsColumnIndex() {
        return tsColumn;
    }

    public void setTsMap(Map<Integer, List<Integer>> tsMap) {
        this.tsMap = tsMap;
    }

    public Map<Integer, List<Integer>> getTsMapParsed() {
        return tsMap;
    }
}
