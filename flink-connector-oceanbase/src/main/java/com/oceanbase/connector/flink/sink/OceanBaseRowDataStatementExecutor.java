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
import com.oceanbase.connector.flink.table.OceanBaseTableSchema;
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

public class OceanBaseRowDataStatementExecutor implements OceanBaseStatementExecutor<RowData> {
    private static final Logger LOG =
            LoggerFactory.getLogger(OceanBaseRowDataStatementExecutor.class);

    private static final long serialVersionUID = 1L;

    private final SinkWriterMetricGroup metricGroup;
    private final OceanBaseWriterOptions options;
    private final OceanBaseTableSchema tableSchema;
    private final OceanBaseConnectionProvider connectionProvider;
    private final OceanBaseConnectionInfo connectionInfo;
    private final String existStatementSql;
    private final String insertStatementSql;
    private final String updateStatementSql;
    private final String deleteStatementSql;
    private final String upsertStatementSql;
    private final String queryMemStoreSql;

    private final List<RowData> buffer = new ArrayList<>();
    private final Map<String, Tuple2<Boolean, RowData>> reduceBuffer = new HashMap<>();

    private transient volatile boolean closed = false;
    private volatile long lastCheckMemStoreTime;

    public OceanBaseRowDataStatementExecutor(
            Sink.InitContext context,
            OceanBaseWriterOptions options,
            OceanBaseTableSchema tableSchema,
            OceanBaseConnectionProvider connectionProvider) {
        this.metricGroup = context.metricGroup();
        this.options = options;
        this.tableSchema = tableSchema;
        this.connectionProvider = connectionProvider;
        this.connectionInfo = connectionProvider.getConnectionInfo();
        this.existStatementSql =
                connectionInfo
                        .getDialect()
                        .getExistStatement(
                                connectionInfo.getTableName(), tableSchema.getKeyFieldNames());
        this.insertStatementSql =
                connectionInfo
                        .getDialect()
                        .getInsertIntoStatement(
                                connectionInfo.getTableName(), tableSchema.getFieldNames());
        this.updateStatementSql =
                connectionInfo
                        .getDialect()
                        .getUpdateStatement(
                                connectionInfo.getTableName(),
                                tableSchema.getFieldNames(),
                                tableSchema.getKeyFieldNames());
        this.deleteStatementSql =
                connectionInfo
                        .getDialect()
                        .getDeleteStatement(
                                connectionInfo.getTableName(), tableSchema.getKeyFieldNames());
        this.upsertStatementSql =
                connectionInfo
                        .getDialect()
                        .getUpsertStatement(
                                connectionInfo.getTableName(),
                                tableSchema.getFieldNames(),
                                tableSchema.getKeyFieldNames());
        this.queryMemStoreSql =
                connectionInfo.getVersion().isV4()
                        ? connectionInfo
                                .getDialect()
                                .getMemStoreExistStatement(options.getMemStoreThreshold())
                        : connectionInfo
                                .getDialect()
                                .getLegacyMemStoreExistStatement(options.getMemStoreThreshold());
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
                    Object obj = getter.getFieldOrNull(row);
                    if (obj == null) {
                        statement.setObject(index++, null);
                    } else {
                        statement.setString(index++, obj.toString());
                    }
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
        }
    }
}
