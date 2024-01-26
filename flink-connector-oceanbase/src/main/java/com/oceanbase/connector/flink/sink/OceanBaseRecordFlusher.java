/*
 * Copyright (c) 2023 OceanBase.
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
import com.oceanbase.connector.flink.dialect.OceanBaseDialect;
import com.oceanbase.connector.flink.table.DataChangeRecord;
import com.oceanbase.connector.flink.table.SchemaChangeRecord;
import com.oceanbase.connector.flink.table.TableId;
import com.oceanbase.connector.flink.table.TableInfo;
import com.oceanbase.connector.flink.utils.TableCache;
import com.oceanbase.partition.calculator.enums.ObServerMode;
import com.oceanbase.partition.calculator.helper.TableEntryExtractor;
import com.oceanbase.partition.calculator.model.TableEntry;
import com.oceanbase.partition.calculator.model.TableEntryKey;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
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

public class OceanBaseRecordFlusher implements RecordFlusher {

    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseRecordFlusher.class);

    private static final long serialVersionUID = 1L;

    private final OceanBaseConnectorOptions options;
    private final OceanBaseConnectionProvider connectionProvider;
    private final OceanBaseDialect dialect;

    private transient TableCache<OceanBaseTablePartInfo> tablePartInfoCache;

    private volatile long lastCheckMemStoreTime;

    public OceanBaseRecordFlusher(OceanBaseConnectorOptions options) {
        this(options, new OceanBaseConnectionProvider(options));
    }

    public OceanBaseRecordFlusher(
            OceanBaseConnectorOptions options, OceanBaseConnectionProvider connectionProvider) {
        this.options = options;
        this.connectionProvider = connectionProvider;
        this.dialect = connectionProvider.getDialect();
    }

    private TableCache<OceanBaseTablePartInfo> getTablePartInfoCache() {
        if (tablePartInfoCache == null) {
            tablePartInfoCache = new TableCache<>();
        }
        return tablePartInfoCache;
    }

    @Override
    public synchronized void flush(SchemaChangeRecord record) throws Exception {
        try (Connection connection = connectionProvider.getConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(record.getSql());
        }
        if (record.shouldRefreshSchema()) {
            getTablePartInfoCache().remove(record.getTableId().identifier());
        }
        LOG.info("Flush SchemaChangeRecord successfully: {}", record);
    }

    @Override
    public synchronized void flush(List<DataChangeRecord> batch) throws Exception {
        if (batch == null || batch.isEmpty()) {
            return;
        }
        checkMemStore();

        TableInfo tableInfo = (TableInfo) batch.get(0).getTable();
        TableId tableId = tableInfo.getTableId();

        if (CollectionUtils.isEmpty(tableInfo.getKey())) {
            if (batch.stream().anyMatch(data -> !data.isUpsert())) {
                throw new IllegalArgumentException(
                        "Table without PK must only contain insert records");
            }
            flush(
                    dialect.getInsertIntoStatement(
                            tableId.getSchemaName(),
                            tableId.getTableName(),
                            tableInfo.getFieldNames()),
                    tableInfo.getFieldNames(),
                    batch);
            return;
        }

        List<DataChangeRecord> upsertBatch = new ArrayList<>();
        List<DataChangeRecord> deleteBatch = new ArrayList<>();
        batch.forEach(
                data -> {
                    if (data.isUpsert()) {
                        upsertBatch.add(data);
                    } else {
                        deleteBatch.add(data);
                    }
                });
        if (!upsertBatch.isEmpty()) {
            flush(
                    dialect.getUpsertStatement(
                            tableId.getSchemaName(),
                            tableId.getTableName(),
                            tableInfo.getFieldNames(),
                            tableInfo.getKey()),
                    tableInfo.getFieldNames(),
                    upsertBatch);
        }
        if (!deleteBatch.isEmpty()) {
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

    private void flush(String sql, List<String> statementFields, List<DataChangeRecord> batch)
            throws Exception {
        Map<Long, List<DataChangeRecord>> group = groupRecords(batch);
        if (group == null) {
            return;
        }
        try (Connection connection = connectionProvider.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)) {
            for (List<DataChangeRecord> groupBatch : group.values()) {
                for (DataChangeRecord record : groupBatch) {
                    for (int i = 0; i < statementFields.size(); i++) {
                        statement.setObject(i + 1, record.getFieldValue(statementFields.get(i)));
                    }
                    statement.addBatch();
                }
                statement.executeBatch();
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
        OceanBaseTablePartInfo tablePartInfo = getTablePartInfo(tableInfo.getTableId());
        if (tablePartInfo == null || MapUtils.isEmpty(tablePartInfo.getPartColumnIndexMap())) {
            return null;
        }
        Object[] obj = new Object[tableInfo.getFieldNames().size()];
        for (Map.Entry<String, Integer> entry : tablePartInfo.getPartColumnIndexMap().entrySet()) {
            obj[entry.getValue()] = record.getFieldValue(entry.getKey());
        }
        return tablePartInfo.getPartIdCalculator().calculatePartId(obj);
    }

    private OceanBaseTablePartInfo getTablePartInfo(TableId tableId) {
        if (!options.getPartitionEnabled()) {
            return null;
        }
        return getTablePartInfoCache()
                .get(
                        tableId.identifier(),
                        () -> queryTablePartInfo(tableId.getSchemaName(), tableId.getTableName()));
    }

    private OceanBaseTablePartInfo queryTablePartInfo(String schemaName, String tableName) {
        /*
         'ob-partition-calculator' requires:
         - non-sys tenant username and password for 4.x and later versions
         - sys tenant username and password for 3.x and early versions
        */
        OceanBaseConnectionProvider.Version version = connectionProvider.getVersion();
        if ((version.isV4() && "sys".equalsIgnoreCase(options.getTenantName()))
                || (!version.isV4() && !"sys".equalsIgnoreCase(options.getTenantName()))) {
            LOG.warn(
                    "Can't query table entry on OceanBase version {} with account of tenant {}.",
                    version.getText(),
                    options.getTenantName());
            return null;
        }

        TableEntryKey tableEntryKey =
                new TableEntryKey(
                        options.getClusterName(),
                        options.getTenantName(),
                        schemaName,
                        tableName,
                        connectionProvider.isMySqlMode()
                                ? ObServerMode.fromMySql(version.getText())
                                : ObServerMode.fromOracle(version.getText()));
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

    @Override
    public void close() throws Exception {
        connectionProvider.close();
        if (tablePartInfoCache != null) {
            tablePartInfoCache.clear();
        }
    }
}
