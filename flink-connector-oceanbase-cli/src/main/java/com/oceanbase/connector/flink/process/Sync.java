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
package com.oceanbase.connector.flink.process;

import com.oceanbase.connector.flink.OceanBaseConnectorOptions;
import com.oceanbase.connector.flink.connection.OceanBaseConnectionProvider;
import com.oceanbase.connector.flink.sink.OceanBaseRecordFlusher;
import com.oceanbase.connector.flink.sink.OceanBaseSink;
import com.oceanbase.connector.flink.source.TableSchema;
import com.oceanbase.connector.flink.table.DataChangeRecord;
import com.oceanbase.connector.flink.table.OceanBaseJsonSerializationSchema;
import com.oceanbase.connector.flink.table.OceanBaseTableSchema;
import com.oceanbase.connector.flink.table.TableId;
import com.oceanbase.connector.flink.table.TableInfo;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.OutputTag;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import static com.oceanbase.connector.flink.utils.OceanBaseCatalogUtils.createDatabase;
import static com.oceanbase.connector.flink.utils.OceanBaseCatalogUtils.createTable;
import static com.oceanbase.connector.flink.utils.OceanBaseCatalogUtils.databaseExists;
import static com.oceanbase.connector.flink.utils.OceanBaseCatalogUtils.tableExists;
import static org.apache.flink.cdc.debezium.utils.JdbcUrlUtils.PROPERTIES_PREFIX;

public abstract class Sync {
    private static final Logger LOG = LoggerFactory.getLogger(Sync.class);

    protected StreamExecutionEnvironment env;
    protected Configuration sourceConfig;
    protected Configuration sinkConfig;

    protected String database;
    protected String tablePrefix;
    protected String tableSuffix;
    protected String includingTables;
    protected String excludingTables;
    protected String multiToOneOrigin;
    protected String multiToOneTarget;
    protected boolean createTableOnly = false;
    protected boolean ignoreDefaultValue;
    protected boolean ignoreIncompatible;

    public Sync setEnv(StreamExecutionEnvironment env) {
        this.env = env;
        return this;
    }

    public Sync setSourceConfig(Configuration sourceConfig) {
        this.sourceConfig = sourceConfig;
        return this;
    }

    public Sync setSinkConfig(Configuration sinkConfig) {
        this.sinkConfig = sinkConfig;
        return this;
    }

    public Sync setDatabase(String database) {
        this.database = database;
        return this;
    }

    public Sync setTablePrefix(String tablePrefix) {
        this.tablePrefix = tablePrefix;
        return this;
    }

    public Sync setTableSuffix(String tableSuffix) {
        this.tableSuffix = tableSuffix;
        return this;
    }

    public Sync setIncludingTables(String includingTables) {
        this.includingTables = includingTables;
        return this;
    }

    public Sync setExcludingTables(String excludingTables) {
        this.excludingTables = excludingTables;
        return this;
    }

    public Sync setMultiToOneOrigin(String multiToOneOrigin) {
        this.multiToOneOrigin = multiToOneOrigin;
        return this;
    }

    public Sync setMultiToOneTarget(String multiToOneTarget) {
        this.multiToOneTarget = multiToOneTarget;
        return this;
    }

    public Sync setCreateTableOnly(boolean createTableOnly) {
        this.createTableOnly = createTableOnly;
        return this;
    }

    public Sync setIgnoreDefaultValue(boolean ignoreDefaultValue) {
        this.ignoreDefaultValue = ignoreDefaultValue;
        return this;
    }

    public Sync setIgnoreIncompatible(boolean ignoreIncompatible) {
        this.ignoreIncompatible = ignoreIncompatible;
        return this;
    }

    protected Pattern includingPattern;
    protected Pattern excludingPattern;
    protected TableNameConverter tableNameConverter;
    protected final Map<String, String> tableMapping = new HashMap<>();

    protected Properties getJdbcProperties() {
        Properties jdbcProps = new Properties();
        for (Map.Entry<String, String> entry : sourceConfig.toMap().entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key.startsWith(PROPERTIES_PREFIX)) {
                jdbcProps.put(key.substring(PROPERTIES_PREFIX.length()), value);
            }
        }
        return jdbcProps;
    }

    protected String getJdbcUrlTemplate(String initialJdbcUrl, Properties jdbcProperties) {
        StringBuilder jdbcUrlBuilder = new StringBuilder(initialJdbcUrl);
        jdbcProperties.forEach(
                (key, value) -> jdbcUrlBuilder.append("&").append(key).append("=").append(value));
        return jdbcUrlBuilder.toString();
    }

    protected boolean isSyncNeeded(String tableName) {
        boolean sync = true;
        if (includingPattern != null) {
            sync = includingPattern.matcher(tableName).matches();
        }
        if (excludingPattern != null) {
            sync = sync && !excludingPattern.matcher(tableName).matches();
        }
        LOG.info("Table {} {}", tableName, sync ? "will be synced" : "is filtered out");
        return sync;
    }

    protected abstract List<TableSchema> getTableSchemas();

    protected abstract DataStreamSource<String> buildSource();

    public void build() {
        this.includingPattern = includingTables == null ? null : Pattern.compile(includingTables);
        this.excludingPattern = excludingTables == null ? null : Pattern.compile(excludingTables);
        this.tableNameConverter =
                new TableNameConverter(
                        tablePrefix, tableSuffix, multiToOneOrigin, multiToOneTarget);

        OceanBaseConnectorOptions connectorOptions =
                new OceanBaseConnectorOptions(sinkConfig.toMap());
        OceanBaseConnectionProvider connectionProvider =
                new OceanBaseConnectionProvider(connectorOptions);

        List<Tuple2<String, String>> targetTables = new ArrayList<>();
        Map<Tuple2<String, String>, TableInfo> tableInfoMap = new HashMap<>();

        for (TableSchema schema : getTableSchemas()) {
            String databaseName = database == null ? schema.getDatabaseName() : database;
            tryCreateDatabaseIfAbsent(connectionProvider, databaseName);

            String targetTable = tableNameConverter.convert(schema.getTableName());
            tableMapping.put(
                    schema.getTableIdentifier(), String.format("%s.%s", databaseName, targetTable));
            tryCreateTableIfAbsent(connectionProvider, databaseName, targetTable, schema);

            Tuple2<String, String> targetTableTuple = new Tuple2<>(databaseName, targetTable);

            if (!targetTables.contains(targetTableTuple)) {
                targetTables.add(targetTableTuple);
                tableInfoMap.put(targetTableTuple, schema.toTableInfo());
            }
        }
        LOG.info("Creating tables finished, tables mapping: {}", tableMapping);

        if (createTableOnly) {
            return;
        }

        DataStreamSource<String> source = buildSource();

        SingleOutputStreamOperator<Void> parsedStream =
                source.process(new ParsingProcessFunction(tableNameConverter));

        for (Tuple2<String, String> dbTbl : targetTables) {
            String tableName = dbTbl.f1;
            OutputTag<String> recordOutputTag =
                    ParsingProcessFunction.createRecordOutputTag(tableName);
            DataStream<String> sideOutput = parsedStream.getSideOutput(recordOutputTag);

            int sinkParallel =
                    sinkConfig.getInteger(
                            FactoryUtil.SINK_PARALLELISM, sideOutput.getParallelism());

            OceanBaseSink<String> sink =
                    new OceanBaseSink<>(
                            connectorOptions,
                            null,
                            new OceanBaseJsonSerializationSchema(tableInfoMap.get(dbTbl)),
                            DataChangeRecord.KeyExtractor.simple(),
                            new OceanBaseRecordFlusher(connectorOptions));

            sideOutput.sinkTo(sink).setParallelism(sinkParallel).name(dbTbl.f1).uid(dbTbl.f1);
        }
    }

    private void tryCreateDatabaseIfAbsent(
            OceanBaseConnectionProvider connectionProvider, String databaseName) {
        if (!databaseExists(connectionProvider, databaseName)) {
            LOG.info("Database {} does not exist, try to create it now", databaseName);
            createDatabase(connectionProvider, databaseName);
            LOG.info("Database {} has been created successfully", databaseName);
        }
    }

    private void tryCreateTableIfAbsent(
            OceanBaseConnectionProvider connectionProvider,
            String databaseName,
            String tableName,
            TableSchema schema) {
        if (!tableExists(connectionProvider, databaseName, tableName)) {
            LOG.info("Table {}.{} does not exist, try to create it now", databaseName, tableName);
            OceanBaseTableSchema oceanbaseSchema =
                    new OceanBaseTableSchema(
                            new TableId(databaseName, tableName),
                            schema.getTableComment(),
                            schema.getFields(),
                            schema.getPrimaryKeys());
            try {
                createTable(connectionProvider, oceanbaseSchema);
                LOG.info("Table {}.{} has been created successfully", databaseName, tableName);
            } catch (Exception ex) {
                handleTableCreationFailure(ex);
            }
        }
    }

    private void handleTableCreationFailure(Exception ex) {
        if (ignoreIncompatible && ex.getCause() instanceof SQLSyntaxErrorException) {
            LOG.warn(
                    "OceanBase schema and source table schema are not compatible. Error: {} ",
                    ex.getCause().toString());
        } else {
            throw new RuntimeException("Failed to create table due to: ", ex);
        }
    }
}
