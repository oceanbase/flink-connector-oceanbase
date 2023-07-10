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

import com.oceanbase.connector.flink.connection.OBKVHBaseConnectionProvider;
import com.oceanbase.connector.flink.connection.OBKVHBaseTableSchema;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
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
                if (record.getRowKind() == RowKind.INSERT
                        || record.getRowKind() == RowKind.UPDATE_AFTER) {
                    Put put = getPut(record);
                    if (put != null) {
                        putList.add(put);
                    }
                } else {
                    Delete delete = getDelete(record);
                    if (delete != null) {
                        deleteList.add(delete);
                    }
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

    private Put getPut(RowData rowData) {
        Object rowKey = tableSchema.getRowKeyFieldGetter().getFieldOrNull(rowData);
        if (rowKey == null) {
            return null;
        }
        Put put = new Put((byte[]) (rowKey));
        for (Map.Entry<String, Map<String, RowData.FieldGetter>> entry :
                tableSchema.getFamilyQualifierFieldGetterMap().entrySet()) {
            String familyName = entry.getKey();
            byte[] family = Bytes.toBytes(familyName);
            int index = tableSchema.getFamilyIndexMap().get(familyName);
            RowData familyData = rowData.getRow(index, entry.getValue().size());
            for (Map.Entry<String, RowData.FieldGetter> subEntry : entry.getValue().entrySet()) {
                String qualifierName = subEntry.getKey();
                byte[] qualifier = Bytes.toBytes(qualifierName);
                byte[] value = (byte[]) (subEntry.getValue().getFieldOrNull(familyData));
                put.add(family, qualifier, value);
            }
        }
        return put;
    }

    private Delete getDelete(RowData rowData) {
        Object rowKey = tableSchema.getRowKeyFieldGetter().getFieldOrNull(rowData);
        if (rowKey == null) {
            return null;
        }
        Delete delete = new Delete((byte[]) rowKey);
        for (Map.Entry<String, Map<String, RowData.FieldGetter>> entry :
                tableSchema.getFamilyQualifierFieldGetterMap().entrySet()) {
            String familyName = entry.getKey();
            byte[] family = Bytes.toBytes(familyName);
            for (Map.Entry<String, RowData.FieldGetter> subEntry : entry.getValue().entrySet()) {
                String qualifierName = subEntry.getKey();
                byte[] qualifier = Bytes.toBytes(qualifierName);
                delete.deleteColumns(family, qualifier);
            }
        }
        return delete;
    }

    @Override
    public void close() throws Exception {
        connectionProvider.close();
    }
}
