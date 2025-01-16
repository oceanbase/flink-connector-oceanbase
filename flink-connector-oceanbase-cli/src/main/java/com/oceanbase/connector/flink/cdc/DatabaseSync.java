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
package com.oceanbase.connector.flink.cdc; // Licensed to the Apache Software Foundation (ASF)

import com.oceanbase.connector.flink.OceanBaseConnectorOptions;
import com.oceanbase.connector.flink.connection.OceanBaseConnectionProvider;
import com.oceanbase.connector.flink.sink.OceanBaseRecordFlusher;
import com.oceanbase.connector.flink.sink.OceanBaseSink;
import com.oceanbase.connector.flink.table.DataChangeRecord;
import com.oceanbase.connector.flink.table.OceanBaseJsonSerializationSchema;
import com.oceanbase.connector.flink.table.OceanBaseSchemaFactory;
import com.oceanbase.connector.flink.table.TableInfo;
import com.oceanbase.connector.flink.table.TableSchema;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import static com.oceanbase.connector.flink.utils.OceanBaseCatalogUtils.createDatabase;
import static com.oceanbase.connector.flink.utils.OceanBaseCatalogUtils.createTable;
import static com.oceanbase.connector.flink.utils.OceanBaseCatalogUtils.databaseExists;
import static com.oceanbase.connector.flink.utils.OceanBaseCatalogUtils.tableExists;
import static org.apache.flink.cdc.debezium.utils.JdbcUrlUtils.PROPERTIES_PREFIX;

public abstract class DatabaseSync {
    private static final Logger LOG = LoggerFactory.getLogger(DatabaseSync.class);
    private static final String TABLE_NAME_OPTIONS = "table-name";

    protected Configuration config;

    protected String database;

    protected TableNameConverter tableNameConverter;
    protected Pattern includingPattern;
    protected Pattern excludingPattern;
    protected Map<Pattern, String> multiToOneRulesPattern;
    protected Configuration sinkConfig;
    protected boolean ignoreDefaultValue;
    protected boolean ignoreIncompatible;

    public StreamExecutionEnvironment env;
    private boolean createTableOnly = false;
    protected String includingTables;
    protected String excludingTables;
    protected String multiToOneOrigin;
    protected String multiToOneTarget;
    protected String tablePrefix;
    protected String tableSuffix;
    protected final Map<String, String> tableMapping = new HashMap<>();
    public static final ConfigOption<Integer> SINK_PARALLELISM = FactoryUtil.SINK_PARALLELISM;

    public abstract void registerDriver() throws SQLException;

    public abstract Connection getConnection() throws SQLException;

    public abstract List<SourceSchema> getSchemaList() throws Exception;

    public abstract DataStreamSource<String> buildCdcSource(StreamExecutionEnvironment env);

    /** Get the prefix of a specific tableList, for example, mysql is database, oracle is schema. */
    public abstract String getTableListPrefix();

    protected DatabaseSync() throws SQLException {
        registerDriver();
    }

    public void create() {
        this.includingPattern = includingTables == null ? null : Pattern.compile(includingTables);
        this.excludingPattern = excludingTables == null ? null : Pattern.compile(excludingTables);
        this.multiToOneRulesPattern = multiToOneRulesParser(multiToOneOrigin, multiToOneTarget);
        this.tableNameConverter =
                new TableNameConverter(tablePrefix, tableSuffix, multiToOneRulesPattern);
    }

    public void build() throws Exception {
        OceanBaseConnectorOptions connectorOptions =
                new OceanBaseConnectorOptions(sinkConfig.toMap());
        OceanBaseConnectionProvider connectionProvider =
                new OceanBaseConnectionProvider(connectorOptions);
        List<SourceSchema> schemaList = getSchemaList();
        Preconditions.checkState(
                !schemaList.isEmpty(),
                "No tables to be synchronized. Please make sure whether the tables that need to be synchronized exist in the corresponding database or schema.");

        if (!StringUtils.isNullOrWhitespaceOnly(database)) {
            tryCreateDatabaseIfAbsent(connectionProvider, database);
        }

        List<String> syncTables = new ArrayList<>();
        List<Tuple2<String, String>> targetTables = new ArrayList<>();
        Map<Tuple2<String, String>, TableInfo> tableInfoMap = new HashMap<>();

        Set<String> targetDbSet = new HashSet<>();
        for (SourceSchema schema : schemaList) {
            syncTables.add(schema.getTableName());

            String targetDb =
                    StringUtils.isNullOrWhitespaceOnly(database)
                            ? schema.getDatabaseName()
                            : database;
            tryCreateDatabaseIfAbsent(connectionProvider, targetDb);
            targetDbSet.add(targetDb);

            String targetTable = tableNameConverter.convert(schema.getTableName());
            // Calculate the mapping relationship between upstream and downstream tables
            tableMapping.put(
                    schema.getTableIdentifier(), String.format("%s.%s", targetDb, targetTable));
            tryCreateTableIfAbsent(connectionProvider, targetDb, targetTable, schema);

            Tuple2<String, String> targetTableTuple = new Tuple2<>(targetDb, targetTable);

            if (!targetTables.contains(targetTableTuple)) {
                targetTables.add(targetTableTuple);
                tableInfoMap.put(targetTableTuple, schema.toTableInfo());
            }
        }
        LOG.info("Creating tables finished, tables mapping: {}", tableMapping);

        if (createTableOnly) {
            return;
        }

        config.setString(TABLE_NAME_OPTIONS, getSyncTableList(syncTables));
        DataStreamSource<String> streamSource = buildCdcSource(env);

        SingleOutputStreamOperator<Void> parsedStream =
                streamSource.process(new ParsingProcessFunction(tableNameConverter));
        for (Tuple2<String, String> dbTbl : targetTables) {
            OutputTag<String> recordOutputTag =
                    ParsingProcessFunction.createRecordOutputTag(dbTbl.f1);
            DataStream<String> sideOutput = parsedStream.getSideOutput(recordOutputTag);
            int sinkParallel = sinkConfig.getInteger(SINK_PARALLELISM, sideOutput.getParallelism());
            String uidName = getUid(targetDbSet, dbTbl);

            OceanBaseSink<String> sink =
                    new OceanBaseSink<>(
                            connectorOptions,
                            null,
                            new OceanBaseJsonSerializationSchema(tableInfoMap.get(dbTbl)),
                            DataChangeRecord.KeyExtractor.simple(),
                            new OceanBaseRecordFlusher(connectorOptions));

            sideOutput.sinkTo(sink).setParallelism(sinkParallel).name(uidName).uid(uidName);
        }
    }

    /**
     * @param targetDbSet The set of target databases.
     * @param dbTbl The database-table tuple.
     * @return The UID of the DataStream.
     */
    public String getUid(Set<String> targetDbSet, Tuple2<String, String> dbTbl) {
        String uid;
        // Determine whether to proceed with multi-database synchronization.
        // if yes, the UID is composed of `dbName_tableName`, otherwise it is composed of
        // `tableName`.
        if (targetDbSet.size() > 1) {
            uid = dbTbl.f0 + "_" + dbTbl.f1;
        } else {
            uid = dbTbl.f1;
        }
        return uid;
    }

    /** Filter table that need to be synchronized. */
    protected boolean isSyncNeeded(String tableName) {
        boolean sync = true;
        if (includingPattern != null) {
            sync = includingPattern.matcher(tableName).matches();
        }
        if (excludingPattern != null) {
            sync = sync && !excludingPattern.matcher(tableName).matches();
        }
        LOG.debug("table {} is synchronized? {}", tableName, sync);
        return sync;
    }

    public String getSyncTableList(List<String> syncTables) {
        // includingTablePattern and ^excludingPattern
        if (includingTables == null) {
            includingTables = ".*";
        }
        String includingPattern =
                String.format("(%s)\\.(%s)", getTableListPrefix(), includingTables);
        if (StringUtils.isNullOrWhitespaceOnly(excludingTables)) {
            return includingPattern;
        } else {
            String excludingPattern =
                    String.format("?!(%s\\.(%s))$", getTableListPrefix(), excludingTables);
            return String.format("(%s)(%s)", excludingPattern, includingPattern);
        }
    }

    /** Filter table that many tables merge to one. */
    public HashMap<Pattern, String> multiToOneRulesParser(
            String multiToOneOrigin, String multiToOneTarget) {
        if (StringUtils.isNullOrWhitespaceOnly(multiToOneOrigin)
                || StringUtils.isNullOrWhitespaceOnly(multiToOneTarget)) {
            return null;
        }
        HashMap<Pattern, String> multiToOneRulesPattern = new HashMap<>();
        String[] origins = multiToOneOrigin.split("\\|");
        String[] targets = multiToOneTarget.split("\\|");
        if (origins.length != targets.length) {
            LOG.error(
                    "param error : multi to one params length are not equal,please check your params.");
            System.exit(1);
        }
        try {
            for (int i = 0; i < origins.length; i++) {
                multiToOneRulesPattern.put(Pattern.compile(origins[i]), targets[i]);
            }
        } catch (Exception e) {
            LOG.error("param error : Your regular expression is incorrect,please check.");
            System.exit(1);
        }
        return multiToOneRulesPattern;
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
            SourceSchema schema) {
        if (!tableExists(connectionProvider, databaseName, tableName)) {
            LOG.info("Table {}.{} does not exist, try to create it now", databaseName, tableName);
            TableSchema oceanbaseSchema =
                    OceanBaseSchemaFactory.createTableSchema(
                            databaseName,
                            tableName,
                            schema.getFields(),
                            schema.getPrimaryKeys(),
                            schema.getTableComment());
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

    public Properties getJdbcProperties() {
        Properties jdbcProps = new Properties();
        for (Map.Entry<String, String> entry : config.toMap().entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key.startsWith(PROPERTIES_PREFIX)) {
                jdbcProps.put(key.substring(PROPERTIES_PREFIX.length()), value);
            }
        }
        return jdbcProps;
    }

    public String getJdbcUrlTemplate(String initialJdbcUrl, Properties jdbcProperties) {
        StringBuilder jdbcUrlBuilder = new StringBuilder(initialJdbcUrl);
        jdbcProperties.forEach(
                (key, value) -> jdbcUrlBuilder.append("&").append(key).append("=").append(value));
        return jdbcUrlBuilder.toString();
    }

    public DatabaseSync setEnv(StreamExecutionEnvironment env) {
        this.env = env;
        return this;
    }

    public DatabaseSync setConfig(Configuration config) {
        this.config = config;
        return this;
    }

    public DatabaseSync setDatabase(String database) {
        this.database = database;
        return this;
    }

    public DatabaseSync setIncludingTables(String includingTables) {
        this.includingTables = includingTables;
        return this;
    }

    public DatabaseSync setExcludingTables(String excludingTables) {
        this.excludingTables = excludingTables;
        return this;
    }

    public DatabaseSync setMultiToOneOrigin(String multiToOneOrigin) {
        this.multiToOneOrigin = multiToOneOrigin;
        return this;
    }

    public DatabaseSync setMultiToOneTarget(String multiToOneTarget) {
        this.multiToOneTarget = multiToOneTarget;
        return this;
    }

    public DatabaseSync setSinkConfig(Configuration sinkConfig) {
        this.sinkConfig = sinkConfig;
        return this;
    }

    public DatabaseSync setIgnoreDefaultValue(boolean ignoreDefaultValue) {
        this.ignoreDefaultValue = ignoreDefaultValue;
        return this;
    }

    public DatabaseSync setCreateTableOnly(boolean createTableOnly) {
        this.createTableOnly = createTableOnly;
        return this;
    }

    public DatabaseSync setTablePrefix(String tablePrefix) {
        this.tablePrefix = tablePrefix;
        return this;
    }

    public DatabaseSync setTableSuffix(String tableSuffix) {
        this.tableSuffix = tableSuffix;
        return this;
    }

    public static class TableNameConverter implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String prefix;
        private final String suffix;
        private Map<Pattern, String> multiToOneRulesPattern;

        TableNameConverter() {
            this("", "");
        }

        TableNameConverter(String prefix, String suffix) {
            this.prefix = prefix == null ? "" : prefix;
            this.suffix = suffix == null ? "" : suffix;
        }

        TableNameConverter(
                String prefix, String suffix, Map<Pattern, String> multiToOneRulesPattern) {
            this.prefix = prefix == null ? "" : prefix;
            this.suffix = suffix == null ? "" : suffix;
            this.multiToOneRulesPattern = multiToOneRulesPattern;
        }

        public String convert(String tableName) {
            if (multiToOneRulesPattern == null) {
                return prefix + tableName + suffix;
            }

            String target = null;

            for (Map.Entry<Pattern, String> patternStringEntry :
                    multiToOneRulesPattern.entrySet()) {
                if (patternStringEntry.getKey().matcher(tableName).matches()) {
                    target = patternStringEntry.getValue();
                }
            }
            /**
             * If multiToOneRulesPattern is not null and target is not assigned, then the
             * synchronization task contains both multi to one and one to one , prefixes and
             * suffixes are added to common one-to-one mapping tables
             */
            if (target == null) {
                return prefix + tableName + suffix;
            }
            return target;
        }
    }
}
