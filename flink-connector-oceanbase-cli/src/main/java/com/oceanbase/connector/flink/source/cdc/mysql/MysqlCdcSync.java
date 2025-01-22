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

import com.oceanbase.connector.flink.process.Sync;
import com.oceanbase.connector.flink.source.FieldSchema;
import com.oceanbase.connector.flink.source.TableSchema;
import com.oceanbase.connector.flink.source.cdc.OceanBaseJsonDeserializationSchema;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSourceBuilder;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceOptions;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffsetBuilder;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.connectors.shaded.org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.table.DebeziumOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.cdc.debezium.utils.JdbcUrlUtils.PROPERTIES_PREFIX;

public class MysqlCdcSync extends Sync {
    private static final Logger LOG = LoggerFactory.getLogger(MysqlCdcSync.class);

    public static final String JDBC_URL_PATTERN =
            "jdbc:mysql://%s:%d/?useInformationSchema=true&nullCatalogMeansCurrent=false&useUnicode=true";

    private static final String SCAN_STARTUP_MODE_VALUE_INITIAL = "initial";
    private static final String SCAN_STARTUP_MODE_VALUE_SNAPSHOT = "snapshot";
    private static final String SCAN_STARTUP_MODE_VALUE_EARLIEST = "earliest-offset";
    private static final String SCAN_STARTUP_MODE_VALUE_LATEST = "latest-offset";
    private static final String SCAN_STARTUP_MODE_VALUE_SPECIFIC_OFFSET = "specific-offset";
    private static final String SCAN_STARTUP_MODE_VALUE_TIMESTAMP = "timestamp";

    private Connection getConnection() throws SQLException {
        Properties jdbcProperties = getJdbcProperties();
        String jdbcUrlTemplate = getJdbcUrlTemplate(JDBC_URL_PATTERN, jdbcProperties);
        String jdbcUrl =
                String.format(
                        jdbcUrlTemplate,
                        sourceConfig.get(MySqlSourceOptions.HOSTNAME),
                        sourceConfig.get(MySqlSourceOptions.PORT));
        return DriverManager.getConnection(
                jdbcUrl,
                sourceConfig.get(MySqlSourceOptions.USERNAME),
                sourceConfig.get(MySqlSourceOptions.PASSWORD));
    }

    @Override
    protected List<TableSchema> getTableSchemas() {
        String databaseName = sourceConfig.get(MySqlSourceOptions.DATABASE_NAME);
        List<TableSchema> schemaList = new ArrayList<>();
        try (Connection conn = getConnection()) {
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet rs = metaData.getCatalogs()) {
                while (rs.next()) {
                    String catalog = rs.getString("TABLE_CAT");
                    if (catalog.matches(databaseName)) {
                        try (ResultSet tables =
                                metaData.getTables(catalog, null, "%", new String[] {"TABLE"})) {
                            while (tables.next()) {
                                String tableName = tables.getString("TABLE_NAME");
                                String tableComment = tables.getString("REMARKS");
                                if (!isSyncNeeded(tableName)) {
                                    continue;
                                }
                                TableSchema tableSchema =
                                        new TableSchema(
                                                databaseName, null, tableName, tableComment);
                                schemaList.add(tableSchema);
                            }
                        }
                    }
                }
            }
            LOG.info(
                    "Found tables: {}",
                    schemaList.stream()
                            .map(schema -> schema.getDatabaseName() + "." + schema.getTableName()));

            MysqlMetadataAccessor metadataAccessor = new MysqlMetadataAccessor(() -> metaData);
            for (TableSchema tableSchema : schemaList) {
                List<FieldSchema> fieldSchemaList =
                        metadataAccessor.getColumnInfo(
                                tableSchema.getDatabaseName(), null, tableSchema.getTableName());
                for (FieldSchema fieldSchema : fieldSchemaList) {
                    tableSchema.addField(fieldSchema);
                }
                metadataAccessor
                        .getPrimaryKeys(
                                tableSchema.getDatabaseName(), null, tableSchema.getTableName())
                        .forEach(tableSchema::addPrimaryKey);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to query table schemas", e);
        }
        return schemaList;
    }

    @Override
    protected DataStreamSource<String> buildSource() {
        String databaseName = sourceConfig.get(MySqlSourceOptions.DATABASE_NAME);
        MySqlSourceBuilder<String> sourceBuilder = MySqlSource.builder();
        sourceBuilder
                .hostname(sourceConfig.get(MySqlSourceOptions.HOSTNAME))
                .port(sourceConfig.get(MySqlSourceOptions.PORT))
                .username(sourceConfig.get(MySqlSourceOptions.USERNAME))
                .password(sourceConfig.get(MySqlSourceOptions.PASSWORD))
                .databaseList(databaseName)
                .tableList(sourceConfig.get(MySqlSourceOptions.TABLE_NAME));

        sourceConfig.getOptional(MySqlSourceOptions.SERVER_ID).ifPresent(sourceBuilder::serverId);
        sourceConfig
                .getOptional(MySqlSourceOptions.SERVER_TIME_ZONE)
                .ifPresent(sourceBuilder::serverTimeZone);
        sourceConfig
                .getOptional(MySqlSourceOptions.SCAN_SNAPSHOT_FETCH_SIZE)
                .ifPresent(sourceBuilder::fetchSize);
        sourceConfig
                .getOptional(MySqlSourceOptions.CONNECT_TIMEOUT)
                .ifPresent(sourceBuilder::connectTimeout);
        sourceConfig
                .getOptional(MySqlSourceOptions.CONNECT_MAX_RETRIES)
                .ifPresent(sourceBuilder::connectMaxRetries);
        sourceConfig
                .getOptional(MySqlSourceOptions.CONNECTION_POOL_SIZE)
                .ifPresent(sourceBuilder::connectionPoolSize);
        sourceConfig
                .getOptional(MySqlSourceOptions.HEARTBEAT_INTERVAL)
                .ifPresent(sourceBuilder::heartbeatInterval);
        sourceConfig
                .getOptional(MySqlSourceOptions.SCAN_NEWLY_ADDED_TABLE_ENABLED)
                .ifPresent(sourceBuilder::scanNewlyAddedTableEnabled);
        sourceConfig
                .getOptional(MySqlSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE)
                .ifPresent(sourceBuilder::splitSize);
        sourceConfig
                .getOptional(MySqlSourceOptions.SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED)
                .ifPresent(sourceBuilder::closeIdleReaders);

        setChunkKeyColumns(sourceBuilder);

        String startupMode = sourceConfig.get(MySqlSourceOptions.SCAN_STARTUP_MODE);
        switch (startupMode.toLowerCase()) {
            case SCAN_STARTUP_MODE_VALUE_INITIAL:
                sourceBuilder.startupOptions(StartupOptions.initial());
                break;

            case SCAN_STARTUP_MODE_VALUE_SNAPSHOT:
                sourceBuilder.startupOptions(StartupOptions.snapshot());
                break;

            case SCAN_STARTUP_MODE_VALUE_LATEST:
                sourceBuilder.startupOptions(StartupOptions.latest());
                break;

            case SCAN_STARTUP_MODE_VALUE_EARLIEST:
                sourceBuilder.startupOptions(StartupOptions.earliest());
                break;

            case SCAN_STARTUP_MODE_VALUE_SPECIFIC_OFFSET:
                BinlogOffsetBuilder offsetBuilder = BinlogOffset.builder();
                String file =
                        sourceConfig.get(MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_FILE);
                Long pos = sourceConfig.get(MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_POS);
                if (file != null && pos != null) {
                    offsetBuilder.setBinlogFilePosition(file, pos);
                }
                sourceConfig
                        .getOptional(MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_GTID_SET)
                        .ifPresent(offsetBuilder::setGtidSet);
                sourceConfig
                        .getOptional(MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_SKIP_EVENTS)
                        .ifPresent(offsetBuilder::setSkipEvents);
                sourceConfig
                        .getOptional(MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_SKIP_ROWS)
                        .ifPresent(offsetBuilder::setSkipRows);
                sourceBuilder.startupOptions(StartupOptions.specificOffset(offsetBuilder.build()));
                break;

            case SCAN_STARTUP_MODE_VALUE_TIMESTAMP:
                sourceBuilder.startupOptions(
                        StartupOptions.timestamp(
                                sourceConfig.get(
                                        MySqlSourceOptions.SCAN_STARTUP_TIMESTAMP_MILLIS)));
                break;

            default:
                throw new ValidationException(
                        String.format(
                                "Invalid value for option '%s'. Supported values are [%s, %s, %s, %s, %s, %s], but was: %s",
                                MySqlSourceOptions.SCAN_STARTUP_MODE.key(),
                                SCAN_STARTUP_MODE_VALUE_INITIAL,
                                SCAN_STARTUP_MODE_VALUE_SNAPSHOT,
                                SCAN_STARTUP_MODE_VALUE_LATEST,
                                SCAN_STARTUP_MODE_VALUE_EARLIEST,
                                SCAN_STARTUP_MODE_VALUE_SPECIFIC_OFFSET,
                                SCAN_STARTUP_MODE_VALUE_TIMESTAMP,
                                startupMode));
        }

        Properties jdbcProperties = new Properties();
        Properties debeziumProperties = new Properties();
        debeziumProperties.putAll(MysqlDateConverter.DEFAULT_PROPS);

        for (Map.Entry<String, String> entry : sourceConfig.toMap().entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key.startsWith(PROPERTIES_PREFIX)) {
                jdbcProperties.put(key.substring(PROPERTIES_PREFIX.length()), value);
            } else if (key.startsWith(DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX)) {
                debeziumProperties.put(
                        key.substring(DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX.length()), value);
            }
        }
        sourceBuilder.jdbcProperties(jdbcProperties);
        sourceBuilder.debeziumProperties(debeziumProperties);

        DebeziumDeserializationSchema<String> schema;
        if (ignoreDefaultValue) {
            schema = new OceanBaseJsonDeserializationSchema();
        } else {
            Map<String, Object> customConverterConfigs = new HashMap<>();
            customConverterConfigs.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, "numeric");
            schema = new JsonDebeziumDeserializationSchema(false, customConverterConfigs);
        }
        MySqlSource<String> mySqlSource =
                sourceBuilder.deserializer(schema).includeSchemaChanges(true).build();

        return env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");
    }

    private void setChunkKeyColumns(MySqlSourceBuilder<String> sourceBuilder) {
        String chunkKeyColumn =
                sourceConfig.getString(
                        MySqlSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN);
        if (!StringUtils.isNullOrWhitespaceOnly(chunkKeyColumn)) {
            final Pattern chunkPattern = Pattern.compile("(\\S+)\\.(\\S+):(\\S+)");
            String[] tblColumns = chunkKeyColumn.split(",");
            for (String tblCol : tblColumns) {
                Matcher matcher = chunkPattern.matcher(tblCol);
                if (matcher.find()) {
                    String db = matcher.group(1);
                    String table = matcher.group(2);
                    String col = matcher.group(3);
                    sourceBuilder.chunkKeyColumn(new ObjectPath(db, table), col);
                }
            }
        }
    }
}
