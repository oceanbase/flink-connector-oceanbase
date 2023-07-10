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

package com.oceanbase.connector.flink.sink;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import com.oceanbase.connector.flink.connection.OceanBaseConnectionInfo;
import com.oceanbase.connector.flink.connection.OceanBaseConnectionProvider;
import com.oceanbase.connector.flink.connection.OceanBaseTablePartInfo;
import com.oceanbase.connector.flink.connection.OceanBaseTableSchema;
import com.oceanbase.connector.flink.dialect.OceanBaseDialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class OceanBaseStatementExecutor implements StatementExecutor<RowData> {
    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseStatementExecutor.class);

    private static final long serialVersionUID = 1L;

    private final OceanBaseStatementOptions options;
    private final OceanBaseTableSchema tableSchema;
    private final OceanBaseConnectionProvider connectionProvider;
    private final String existStatementSql;
    private final String insertStatementSql;
    private final String updateStatementSql;
    private final String deleteStatementSql;
    private final String upsertStatementSql;
    private final String queryMemStoreSql;

    private final OceanBaseTablePartInfo tablePartInfo;
    private final List<Integer> partColumnIndexes;
    private final ExecutorService statementExecutorService;

    private final List<RowData> buffer = new ArrayList<>();
    private final Map<String, Tuple2<Boolean, RowData>> reduceBuffer = new HashMap<>();

    private transient volatile boolean closed = false;
    private volatile long lastCheckMemStoreTime;
    private volatile SQLException statementException;
    private SinkWriterMetricGroup metricGroup;

    public OceanBaseStatementExecutor(
            OceanBaseStatementOptions options,
            OceanBaseTableSchema tableSchema,
            OceanBaseConnectionProvider connectionProvider) {
        this.options = options;
        this.tableSchema = tableSchema;
        this.connectionProvider = connectionProvider;

        OceanBaseConnectionInfo connectionInfo = connectionProvider.getConnectionInfo();
        OceanBaseDialect dialect = connectionInfo.getDialect();
        String schemaName = connectionInfo.getTableEntryKey().getSchemaName();
        String tableName = connectionInfo.getTableEntryKey().getTableName();

        this.existStatementSql =
                dialect.getExistStatement(schemaName, tableName, tableSchema.getKeyFieldNames());
        this.insertStatementSql =
                dialect.getInsertIntoStatement(schemaName, tableName, tableSchema.getFieldNames());
        this.updateStatementSql =
                dialect.getUpdateStatement(
                        schemaName,
                        tableName,
                        tableSchema.getFieldNames(),
                        tableSchema.getKeyFieldNames());
        this.deleteStatementSql =
                dialect.getDeleteStatement(schemaName, tableName, tableSchema.getKeyFieldNames());
        this.upsertStatementSql =
                dialect.getUpsertStatement(
                        schemaName,
                        tableName,
                        tableSchema.getFieldNames(),
                        tableSchema.getKeyFieldNames());
        this.queryMemStoreSql =
                connectionInfo.getVersion().isV4()
                        ? dialect.getMemStoreExistStatement(options.getMemStoreThreshold())
                        : dialect.getLegacyMemStoreExistStatement(options.getMemStoreThreshold());
        this.tablePartInfo =
                options.isPartitionEnabled() ? connectionProvider.getTablePartInfo() : null;
        if (this.tablePartInfo != null && !this.tablePartInfo.getPartColumnIndexMap().isEmpty()) {
            LOG.info(
                    "Table partition info loaded, part columns: {}",
                    tablePartInfo.getPartColumnIndexMap().keySet());

            this.partColumnIndexes =
                    IntStream.range(0, tableSchema.getFieldNames().size())
                            .filter(
                                    i ->
                                            tablePartInfo
                                                    .getPartColumnIndexMap()
                                                    .containsKey(
                                                            tableSchema.getFieldNames().get(i)))
                            .boxed()
                            .collect(Collectors.toList());

            if (options.getPartitionNumber() > 1) {
                this.statementExecutorService =
                        Executors.newFixedThreadPool(options.getPartitionNumber());
                LOG.info(
                        "ExecutorService set with {} threads, will flush records by partitions in parallel",
                        options.getPartitionNumber());
            } else {
                this.statementExecutorService = null;
                LOG.info(
                        "ExecutorService not set, will flush records by partitions in main thread");
            }
        } else {
            this.partColumnIndexes = null;
            this.statementExecutorService = null;
            LOG.info("No table partition info loaded, will flush records directly");
        }
    }

    @Override
    public void setContext(Sink.InitContext context) {
        this.metricGroup = context.metricGroup();
    }

    @Override
    public void addToBatch(RowData record) {
        if (record.getArity() != tableSchema.getFieldNames().size()) {
            throw new RuntimeException(
                    "Record fields number "
                            + record.getArity()
                            + "doesn't match sql columns "
                            + tableSchema.getFieldNames().size());
        }
        boolean flag = changeFlag(record.getRowKind());
        if (!tableSchema.isHasKey() && flag) {
            synchronized (buffer) {
                buffer.add(record);
            }
        } else {
            String key = constructKey(record, tableSchema.getKeyFieldGetters());
            synchronized (reduceBuffer) {
                reduceBuffer.put(key, Tuple2.of(flag, record));
            }
        }
        metricGroup.getIOMetricGroup().getNumRecordsInCounter().inc();
    }

    /**
     * Returns true if the row kind is INSERT or UPDATE_AFTER, returns false if the row kind is
     * DELETE or UPDATE_BEFORE.
     *
     * @param rowKind row kind
     * @return change flag
     */
    private boolean changeFlag(RowKind rowKind) {
        switch (rowKind) {
            case INSERT:
            case UPDATE_AFTER:
                return true;
            case DELETE:
            case UPDATE_BEFORE:
                return false;
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Unknown row kind, the supported row kinds is: INSERT, UPDATE_BEFORE, UPDATE_AFTER,"
                                        + " DELETE, but get: %s.",
                                rowKind));
        }
    }

    /**
     * Constructs a unique key for the record
     *
     * @param rowData row data
     * @param keyFieldGetters the field getters of key fields
     * @return key string
     */
    private String constructKey(RowData rowData, RowData.FieldGetter[] keyFieldGetters) {
        StringBuilder key = new StringBuilder();
        for (RowData.FieldGetter fieldGetter : keyFieldGetters) {
            Object obj = fieldGetter.getFieldOrNull(rowData);
            key.append(obj == null ? "null" : obj.toString());
            key.append("#");
        }
        return key.toString();
    }

    @Override
    public void executeBatch() throws SQLException {
        if (options.isMemStoreCheckEnabled()) {
            checkMemStore();
        }
        if (!tableSchema.isHasKey()) {
            synchronized (buffer) {
                executeBatch(insertStatementSql, buffer, tableSchema.getFieldGetters());
                buffer.clear();
            }
        } else {
            synchronized (reduceBuffer) {
                List<RowData> writeBuffer = new ArrayList<>();
                List<RowData> deleteBuffer = new ArrayList<>();
                for (Tuple2<Boolean, RowData> tuple2 : reduceBuffer.values()) {
                    if (tuple2.f0) {
                        writeBuffer.add(tuple2.f1);
                    } else {
                        deleteBuffer.add(tuple2.f1);
                    }
                }
                if (options.isUpsertMode()) {
                    executeBatch(upsertStatementSql, writeBuffer, tableSchema.getFieldGetters());
                } else {
                    List<RowData> updateBuffer = new ArrayList<>();
                    List<RowData> insertBuffer = new ArrayList<>();
                    for (RowData rowData : writeBuffer) {
                        if (closed) {
                            break;
                        }
                        if (exist(rowData)) {
                            updateBuffer.add(rowData);
                        } else {
                            insertBuffer.add(rowData);
                        }
                    }
                    executeBatch(
                            updateStatementSql,
                            updateBuffer,
                            tableSchema.getNonKeyFieldGetters(),
                            tableSchema.getKeyFieldGetters());
                    executeBatch(insertStatementSql, insertBuffer, tableSchema.getFieldGetters());
                }
                executeBatch(deleteStatementSql, deleteBuffer, tableSchema.getKeyFieldGetters());
                reduceBuffer.clear();
            }
        }
    }

    private void checkMemStore() throws SQLException {
        long now = System.currentTimeMillis();
        if (closed || now - lastCheckMemStoreTime < options.getMemStoreCheckInterval()) {
            return;
        }
        while (!closed && hasMemStoreReachedThreshold()) {
            LOG.warn(
                    "Memstore reaches threshold {}, thread will sleep {} milliseconds",
                    options.getMemStoreThreshold(),
                    options.getMemStoreCheckInterval());
            try {
                Thread.sleep(options.getMemStoreCheckInterval());
            } catch (InterruptedException e) {
                LOG.warn(e.getMessage());
            }
        }
        lastCheckMemStoreTime = System.currentTimeMillis();
    }

    private boolean hasMemStoreReachedThreshold() throws SQLException {
        try (Connection connection = connectionProvider.getConnection();
                Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(queryMemStoreSql);
            return resultSet.next();
        }
    }

    /**
     * Checks whether the row data exists
     *
     * @param rowData row data
     * @return true if the row data exists
     * @throws SQLException if a database access error occurs
     */
    private boolean exist(RowData rowData) throws SQLException {
        if (closed || rowData == null) {
            return true;
        }
        try (Connection connection = connectionProvider.getConnection();
                PreparedStatement statement = connection.prepareStatement(existStatementSql)) {
            setStatementData(statement, rowData, tableSchema.getKeyFieldGetters());
            ResultSet resultSet = statement.executeQuery();
            return resultSet.next();
        }
    }

    private void executeBatch(
            String sql, List<RowData> rowDataList, RowData.FieldGetter[]... fieldGetters)
            throws SQLException {
        if (closed || rowDataList == null || rowDataList.isEmpty()) {
            return;
        }
        if (partColumnIndexes != null) {
            Map<Long, List<RowData>> partRowDataMap = new HashMap<>();
            for (RowData rowData : rowDataList) {
                Object[] record = new Object[tableSchema.getFieldNames().size()];
                for (Integer i : partColumnIndexes) {
                    Integer columnIndex =
                            tablePartInfo
                                    .getPartColumnIndexMap()
                                    .get(tableSchema.getFieldNames().get(i));
                    record[columnIndex] = tableSchema.getFieldGetters()[i].getFieldOrNull(rowData);
                }
                Long partId = tablePartInfo.getPartIdCalculator().calculatePartId(record);
                partRowDataMap.computeIfAbsent(partId, k -> new ArrayList<>()).add(rowData);
            }
            if (statementExecutorService != null) {
                CountDownLatch latch = new CountDownLatch(partRowDataMap.size());
                for (List<RowData> partRowDataList : partRowDataMap.values()) {
                    statementExecutorService.execute(
                            () -> {
                                try {
                                    execute(sql, partRowDataList, fieldGetters);
                                } catch (SQLException e) {
                                    statementException = e;
                                } finally {
                                    latch.countDown();
                                }
                            });
                }
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException("Statement executor interrupted: " + e.getMessage());
                }
                if (statementException != null) {
                    throw statementException;
                }
            } else {
                for (List<RowData> partRowDataList : partRowDataMap.values()) {
                    execute(sql, partRowDataList, fieldGetters);
                }
            }
        } else {
            execute(sql, rowDataList, fieldGetters);
        }
    }

    private void execute(
            String sql, List<RowData> rowDataList, RowData.FieldGetter[]... fieldGetters)
            throws SQLException {
        try (Connection connection = connectionProvider.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)) {
            int count = 0;
            for (RowData rowData : rowDataList) {
                setStatementData(statement, rowData, fieldGetters);
                statement.addBatch();
                count++;
                if (!closed && count >= options.getBatchSize()) {
                    statement.executeBatch();
                    metricGroup.getIOMetricGroup().getNumRecordsOutCounter().inc(count);
                    count = 0;
                }
            }
            if (!closed && count > 0) {
                statement.executeBatch();
                metricGroup.getIOMetricGroup().getNumRecordsOutCounter().inc(count);
            }
        }
    }

    private void setStatementData(
            PreparedStatement statement, RowData row, RowData.FieldGetter[]... fieldGetters)
            throws SQLException {
        if (row != null) {
            int index = 1;
            for (RowData.FieldGetter[] fieldGetter : fieldGetters) {
                for (RowData.FieldGetter getter : fieldGetter) {
                    statement.setObject(index++, getter.getFieldOrNull(row));
                }
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (!closed) {
            closed = true;

            if (connectionProvider != null) {
                connectionProvider.close();
            }

            if (statementExecutorService != null) {
                statementExecutorService.shutdown();
            }
        }
    }
}
