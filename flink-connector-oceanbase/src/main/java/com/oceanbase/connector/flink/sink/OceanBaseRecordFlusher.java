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

package com.oceanbase.connector.flink.sink;

import com.oceanbase.connector.flink.OceanBaseConnectorOptions;
import com.oceanbase.connector.flink.connection.OceanBaseConnectionProvider;
import com.oceanbase.connector.flink.connection.OceanBaseTablePartInfo;
import com.oceanbase.connector.flink.connection.OceanBaseUserInfo;
import com.oceanbase.connector.flink.connection.OceanBaseVersion;
import com.oceanbase.connector.flink.dialect.OceanBaseDialect;
import com.oceanbase.connector.flink.dialect.OceanBaseMySQLDialect;
import com.oceanbase.connector.flink.table.DataChangeRecord;
import com.oceanbase.connector.flink.table.SchemaChangeRecord;
import com.oceanbase.connector.flink.table.TableId;
import com.oceanbase.connector.flink.table.TableInfo;
import com.oceanbase.connector.flink.table.TransactionRecord;
import com.oceanbase.connector.flink.utils.TableCache;
import com.oceanbase.partition.calculator.enums.ObServerMode;
import com.oceanbase.partition.calculator.helper.TableEntryExtractor;
import com.oceanbase.partition.calculator.model.TableEntry;
import com.oceanbase.partition.calculator.model.TableEntryKey;

import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObjType;
import com.alipay.oceanbase.rpc.table.ObDirectLoadBucket;
import com.alipay.oceanbase.rpc.table.ObTableDirectLoad;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class OceanBaseRecordFlusher implements RecordFlusher {

    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseRecordFlusher.class);

    private static final long serialVersionUID = 1L;

    private final OceanBaseConnectorOptions options;
    private final OceanBaseConnectionProvider connectionProvider;
    private final OceanBaseDialect dialect;

    private final TableCache<OceanBaseTablePartInfo> tablePartInfoCache;
    private final TableCache<ObTableDirectLoad> directLoadCache;

    private volatile long lastCheckMemStoreTime;

    public OceanBaseRecordFlusher(OceanBaseConnectorOptions options) {
        this(options, new OceanBaseConnectionProvider(options));
    }

    public OceanBaseRecordFlusher(
            OceanBaseConnectorOptions options, OceanBaseConnectionProvider connectionProvider) {
        this.options = options;
        this.connectionProvider = connectionProvider;
        this.dialect = connectionProvider.getDialect();
        this.tablePartInfoCache = new TableCache<>();
        this.directLoadCache = new TableCache<>();
    }

    @Override
    public void flush(@Nonnull TransactionRecord record) throws Exception {
        if (options.getDirectLoadEnabled()) {
            switch (record.getType()) {
                case BEGIN:
                    getTableDirectLoad(record.getTableId()).begin();
                    break;
                case COMMIT:
                    getTableDirectLoad(record.getTableId()).commit();
                    break;
                case ROLLBACK:
                    getTableDirectLoad(record.getTableId()).abort();
                    break;
                default:
                    throw new IllegalArgumentException(
                            "Unsupported transaction record type: " + record.getType());
            }
        }
    }

    @Override
    public synchronized void flush(@Nonnull SchemaChangeRecord record) throws Exception {
        if (options.getDirectLoadEnabled()) {
            throw new UnsupportedOperationException();
        }

        try (Connection connection = connectionProvider.getConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(record.getSql());
        }
        if (record.shouldRefreshSchema()) {
            tablePartInfoCache.remove(record.getTableId().identifier());
        }
        LOG.info("Flush SchemaChangeRecord successfully: {}", record);
    }

    @Override
    public synchronized void flush(List<DataChangeRecord> records) throws Exception {
        if (records == null || records.isEmpty()) {
            return;
        }
        checkMemStore();

        TableInfo tableInfo = (TableInfo) records.get(0).getTable();
        TableId tableId = tableInfo.getTableId();

        List<DataChangeRecord> upsertBatch = new ArrayList<>();
        List<DataChangeRecord> deleteBatch = new ArrayList<>();
        records.forEach(
                data -> {
                    if (data.isUpsert()) {
                        upsertBatch.add(data);
                    } else {
                        deleteBatch.add(data);
                    }
                });
        if (!upsertBatch.isEmpty()) {
            if (options.getDirectLoadEnabled()) {
                directLoad(tableId, tableInfo.getFieldNames(), upsertBatch);
            } else if (CollectionUtils.isEmpty(tableInfo.getKey())) {
                flush(
                        dialect.getInsertIntoStatement(
                                tableId.getSchemaName(),
                                tableId.getTableName(),
                                tableInfo.getFieldNames(),
                                tableInfo.getPlaceholderFunc()),
                        tableInfo.getFieldNames(),
                        upsertBatch);
            } else {
                flush(
                        dialect.getUpsertStatement(
                                tableId.getSchemaName(),
                                tableId.getTableName(),
                                tableInfo.getFieldNames(),
                                tableInfo.getKey(),
                                tableInfo.getPlaceholderFunc()),
                        tableInfo.getFieldNames(),
                        upsertBatch);
            }
        }
        if (!deleteBatch.isEmpty()) {
            if (CollectionUtils.isEmpty(tableInfo.getKey())) {
                throw new RuntimeException(
                        "There should be no delete records when the table does not contain PK");
            }
            if (options.getDirectLoadEnabled()) {
                throw new RuntimeException(
                        "There should be no delete records when direct load is enabled");
            }

            flush(
                    dialect.getDeleteStatement(
                            tableId.getSchemaName(), tableId.getTableName(), tableInfo.getKey()),
                    tableInfo.getKey(),
                    deleteBatch);
        }
    }

    private void checkMemStore() throws SQLException {
        if (!options.getMemStoreCheckEnabled()) {
            return;
        }
        long now = System.currentTimeMillis();
        if (lastCheckMemStoreTime != 0
                && now - lastCheckMemStoreTime < options.getMemStoreCheckInterval()) {
            return;
        }
        while (hasMemStoreReachedThreshold()) {
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
        String queryMemStoreSql =
                connectionProvider.getVersion().isV4()
                        ? dialect.getMemStoreExistStatement(options.getMemStoreThreshold())
                        : dialect.getLegacyMemStoreExistStatement(options.getMemStoreThreshold());
        LOG.debug("Check memstore with sql: {}", queryMemStoreSql);
        try (Connection connection = connectionProvider.getConnection();
                Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(queryMemStoreSql);
            return resultSet.next();
        }
    }

    private void directLoad(TableId tableId, List<String> fields, List<DataChangeRecord> records)
            throws Exception {
        ObDirectLoadBucket bucket = new ObDirectLoadBucket();
        for (DataChangeRecord record : records) {
            bucket.addRow(
                    fields.stream()
                            .map(f -> toObObj(record.getFieldValue(f)))
                            .toArray(ObObj[]::new));
        }
        getTableDirectLoad(tableId).insert(bucket);
    }

    private ObTableDirectLoad getTableDirectLoad(TableId tableId) {
        return directLoadCache.get(
                tableId.identifier(), () -> connectionProvider.getDirectLoad(tableId));
    }

    private ObObj toObObj(Object value) {
        Object obObjValue = toObObjValue(value);
        return new ObObj(ObObjType.valueOfType(obObjValue).getDefaultObjMeta(), obObjValue);
    }

    private Object toObObjValue(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof Time) {
            return new Timestamp(((Time) obj).getTime());
        }
        if (obj instanceof BigDecimal || obj instanceof BigInteger) {
            return obj.toString();
        }
        return obj;
    }

    private void flush(String sql, List<String> statementFields, List<DataChangeRecord> records)
            throws Exception {
        Map<Long, List<DataChangeRecord>> group = groupRecords(records);
        if (group == null) {
            return;
        }
        try (Connection connection = connectionProvider.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)) {
            for (List<DataChangeRecord> groupRecords : group.values()) {
                try {
                    for (DataChangeRecord record : groupRecords) {
                        for (int i = 0; i < statementFields.size(); i++) {
                            statement.setObject(
                                    i + 1, record.getFieldValue(statementFields.get(i)));
                        }
                        statement.addBatch();
                    }
                    statement.executeBatch();
                } catch (SQLException e) {
                    throw new RuntimeException(
                            "Failed to execute batch with sql: "
                                    + sql
                                    + ", records: "
                                    + groupRecords,
                            e);
                }
            }
        }
    }

    private Map<Long, List<DataChangeRecord>> groupRecords(List<DataChangeRecord> records) {
        if (CollectionUtils.isEmpty(records)) {
            return null;
        }
        Map<Long, List<DataChangeRecord>> group = new HashMap<>();
        for (DataChangeRecord record : records) {
            Long partId = getPartId(record);
            group.computeIfAbsent(partId == null ? -1 : partId, k -> new ArrayList<>()).add(record);
        }
        return group;
    }

    private Long getPartId(DataChangeRecord record) {
        TableInfo tableInfo = (TableInfo) record.getTable();
        OceanBaseTablePartInfo tablePartInfo = getTablePartInfo(tableInfo);
        if (tablePartInfo == null || MapUtils.isEmpty(tablePartInfo.getPartColumnIndexMap())) {
            return null;
        }
        Object[] obj = new Object[tableInfo.getFieldNames().size()];
        for (Map.Entry<String, Integer> entry : tablePartInfo.getPartColumnIndexMap().entrySet()) {
            obj[entry.getValue()] = record.getFieldValue(entry.getKey());
        }
        return tablePartInfo.getPartIdCalculator().calculatePartId(obj);
    }

    private OceanBaseTablePartInfo getTablePartInfo(TableInfo tableInfo) {
        if (options.getSyncWrite() || !options.getPartitionEnabled()) {
            return null;
        }
        return tablePartInfoCache.get(
                tableInfo.getTableId().identifier(),
                () -> {
                    OceanBaseTablePartInfo tablePartInfo =
                            queryTablePartInfo(
                                    tableInfo.getTableId().getSchemaName(),
                                    tableInfo.getTableId().getTableName());
                    verifyTablePartInfo(tableInfo, tablePartInfo);
                    return tablePartInfo;
                });
    }

    private OceanBaseTablePartInfo queryTablePartInfo(String schemaName, String tableName) {
        /*
         'ob-partition-calculator' requires:
         - non-sys tenant username and password for 4.x and later versions
         - sys tenant username and password for 3.x and early versions
        */
        OceanBaseVersion version = connectionProvider.getVersion();
        OceanBaseUserInfo userInfo = connectionProvider.getUserInfo();
        if ((version.isV4() && "sys".equalsIgnoreCase(userInfo.getTenant()))
                || (!version.isV4() && !"sys".equalsIgnoreCase(userInfo.getTenant()))) {
            LOG.warn(
                    "Can't query table entry on OceanBase version {} with account of tenant {}.",
                    version.getVersion(),
                    userInfo.getTenant());
            return null;
        }

        TableEntryKey tableEntryKey =
                new TableEntryKey(
                        userInfo.getCluster(),
                        userInfo.getTenant(),
                        schemaName,
                        tableName,
                        dialect instanceof OceanBaseMySQLDialect
                                ? ObServerMode.fromMySql(version.getVersion())
                                : ObServerMode.fromOracle(version.getVersion()));
        LOG.debug("Query table entry by tableEntryKey: {}", tableEntryKey);
        try (Connection connection = connectionProvider.getConnection()) {
            TableEntry tableEntry =
                    new TableEntryExtractor()
                            .queryTableEntry(connection, tableEntryKey, version.isV4());
            if (tableEntry == null) {
                throw new RuntimeException("Failed to get table entry with key: " + tableEntryKey);
            }
            LOG.info("Query tableEntry by {}, got: {}", tableEntryKey, tableEntry);
            return new OceanBaseTablePartInfo(tableEntry, version.isV4());
        } catch (Exception e) {
            throw new RuntimeException("Failed to get table partition info", e);
        }
    }

    private void verifyTablePartInfo(TableInfo tableInfo, OceanBaseTablePartInfo tablePartInfo) {
        if (tablePartInfo == null) {
            return;
        }
        List<String> columns =
                tablePartInfo.getPartColumnIndexMap().keySet().stream()
                        .filter(partCol -> !tableInfo.getFieldNames().contains(partCol))
                        .collect(Collectors.toList());
        if (!columns.isEmpty()) {
            throw new RuntimeException(
                    "Table "
                            + tableInfo.getTableId().identifier()
                            + " does not contain partition columns: "
                            + columns);
        }
    }

    @Override
    public void close() throws Exception {
        connectionProvider.close();
        if (tablePartInfoCache != null) {
            tablePartInfoCache.clear();
        }
        if (directLoadCache != null) {
            for (ObTableDirectLoad directLoad : directLoadCache.getAll()) {
                directLoad.getTable().close();
            }
            directLoadCache.clear();
        }
    }
}
