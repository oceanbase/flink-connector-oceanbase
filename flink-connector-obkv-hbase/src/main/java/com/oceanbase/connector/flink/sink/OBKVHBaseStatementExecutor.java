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
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import com.oceanbase.connector.flink.connection.OBKVHBaseConnectionProvider;
import com.oceanbase.connector.flink.connection.OBKVHBaseTableSchema;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class OBKVHBaseStatementExecutor implements StatementExecutor<RowData> {

    private static final long serialVersionUID = 1L;

    private final OBKVHBaseStatementOptions options;
    private final OBKVHBaseTableSchema tableSchema;
    private final OBKVHBaseConnectionProvider connectionProvider;

    private final List<RowData> buffer = new ArrayList<>();

    private SinkWriterMetricGroup metricGroup;

    public OBKVHBaseStatementExecutor(
            OBKVHBaseStatementOptions options,
            OBKVHBaseTableSchema tableSchema,
            OBKVHBaseConnectionProvider connectionProvider) {
        this.options = options;
        this.tableSchema = tableSchema;
        this.connectionProvider = connectionProvider;
    }

    @Override
    public void setContext(Sink.InitContext context) {
        this.metricGroup = context.metricGroup();
    }

    @Override
    public void addToBatch(RowData record) {
        synchronized (buffer) {
            buffer.add(record);
        }
        metricGroup.getIOMetricGroup().getNumRecordsInCounter().inc();
    }

    @Override
    public void executeBatch() throws Exception {
        synchronized (buffer) {
            int count = 0;
            List<Put> putList = new LinkedList<>();
            List<Delete> deleteList = new LinkedList<>();
            for (RowData record : buffer) {
                byte[] rowKey = (byte[]) getRowKey(record);
                if (record.getRowKind() == RowKind.INSERT
                        || record.getRowKind() == RowKind.UPDATE_AFTER) {
                    putList.add(getPut(record, rowKey));
                } else {
                    deleteList.add(getDelete(record, rowKey));
                }
                count++;
                if (count >= options.getBatchSize()) {
                    if (!putList.isEmpty()) {
                        connectionProvider.getTable().put(putList);
                    }
                    if (!deleteList.isEmpty()) {
                        connectionProvider.getTable().delete(deleteList);
                    }
                    metricGroup.getIOMetricGroup().getNumRecordsOutCounter().inc(count);
                    count = 0;
                }
            }
            if (count > 0) {
                connectionProvider.getTable().put(putList);
                connectionProvider.getTable().delete(deleteList);
                metricGroup.getIOMetricGroup().getNumRecordsOutCounter().inc(count);
            }
            buffer.clear();
        }
    }

    private Object getRowKey(RowData rowData) throws JsonProcessingException {
        Object rowKey = tableSchema.getRowKeyFieldGetter().getFieldOrNull(rowData);
        if (rowKey == null) {
            throw new RuntimeException(
                    "Failed to get row key from row: "
                            + new ObjectMapper().writeValueAsString(rowData));
        }
        return rowKey;
    }

    private Put getPut(RowData rowData, byte[] rowKey) {
        Put put = new Put(rowKey);
        for (String familyName : tableSchema.getFamilyQualifierFieldGetterMap().keySet()) {
            Map<byte[], byte[]> qualifierValueMap = getQualifierValueMap(rowData, familyName);
            qualifierValueMap.forEach((k, v) -> put.add(Bytes.toBytes(familyName), k, v));
        }
        return put;
    }

    private Delete getDelete(RowData rowData, byte[] rowKey) {
        Delete delete = new Delete(rowKey);
        for (String familyName : tableSchema.getFamilyQualifierFieldGetterMap().keySet()) {
            Map<byte[], byte[]> qualifierValueMap = getQualifierValueMap(rowData, familyName);
            qualifierValueMap.forEach((k, v) -> delete.deleteColumns(Bytes.toBytes(familyName), k));
        }
        return delete;
    }

    private Map<byte[], byte[]> getQualifierValueMap(RowData rowData, String familyName) {
        Map<byte[], byte[]> map = new HashMap<>();
        int familyIndex = tableSchema.getFamilyIndexMap().get(familyName);
        if (rowData.isNullAt(familyIndex)) {
            return map;
        }
        Map<String, RowData.FieldGetter> qualifierFieldGetterMap =
                tableSchema.getFamilyQualifierFieldGetterMap().get(familyName);
        RowData familyData = rowData.getRow(familyIndex, qualifierFieldGetterMap.size());
        for (Map.Entry<String, RowData.FieldGetter> entry : qualifierFieldGetterMap.entrySet()) {
            byte[] value = (byte[]) (entry.getValue().getFieldOrNull(familyData));
            if (value == null) {
                continue;
            }
            map.put(Bytes.toBytes(entry.getKey()), value);
        }
        return map;
    }

    @Override
    public void close() throws Exception {
        connectionProvider.close();
    }
}
