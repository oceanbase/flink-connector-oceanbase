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

import com.oceanbase.connector.flink.connection.OBKVHBaseConnectionProvider;
import com.oceanbase.connector.flink.connection.OBKVHBaseTableSchema;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OBKVHBaseStatementExecutor implements StatementExecutor<RowData> {

    private static final long serialVersionUID = 1L;

    private final OBKVHBaseStatementOptions options;
    private final OBKVHBaseTableSchema tableSchema;
    private final OBKVHBaseConnectionProvider connectionProvider;

    private final List<RowData> buffer = new ArrayList<>();

    private transient volatile boolean closed = false;

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
        if (closed) {
            return;
        }
        synchronized (buffer) {
            int count = 0;
            Map<byte[], List<Put>> familyPutListMap = new HashMap<>();
            Map<byte[], List<Delete>> familyDeleteListMap = new HashMap<>();

            for (RowData record : buffer) {
                byte[] rowKey = getRowKey(record);
                for (String familyName : tableSchema.getFamilyQualifierFieldGetterMap().keySet()) {
                    byte[] family = Bytes.toBytes(familyName);
                    Map<byte[], byte[]> qualifierValueMap =
                            getQualifierValueMap(record, familyName);
                    if (record.getRowKind() == RowKind.INSERT
                            || record.getRowKind() == RowKind.UPDATE_AFTER) {
                        qualifierValueMap.forEach(
                                (qualifier, value) ->
                                        familyPutListMap
                                                .computeIfAbsent(family, k -> new ArrayList<>())
                                                .add(
                                                        new Put(rowKey)
                                                                .add(family, qualifier, value)));
                    } else {
                        qualifierValueMap.forEach(
                                (qualifier, value) ->
                                        familyDeleteListMap
                                                .computeIfAbsent(family, k -> new ArrayList<>())
                                                .add(
                                                        new Delete(rowKey)
                                                                .deleteColumns(family, qualifier)));
                    }
                }
                count++;

                if (!closed && count >= options.getBatchSize()) {
                    flushToTable(familyPutListMap, familyDeleteListMap);
                    metricGroup.getIOMetricGroup().getNumRecordsOutCounter().inc(count);
                    count = 0;
                }
            }
            if (!closed && count > 0) {
                flushToTable(familyPutListMap, familyDeleteListMap);
                metricGroup.getIOMetricGroup().getNumRecordsOutCounter().inc(count);
            }
            buffer.clear();
        }
    }

    private byte[] getRowKey(RowData rowData) throws JsonProcessingException {
        Object rowKey = tableSchema.getRowKeyFieldGetter().getFieldOrNull(rowData);
        if (rowKey == null) {
            throw new RuntimeException(
                    "Failed to get row key from row: "
                            + new ObjectMapper().writeValueAsString(rowData));
        }
        return (byte[]) rowKey;
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

    private void flushToTable(
            Map<byte[], List<Put>> familyPutListMap, Map<byte[], List<Delete>> familyDeleteListMap)
            throws Exception {
        for (List<Put> putList : familyPutListMap.values()) {
            if (CollectionUtils.isNotEmpty(putList)) {
                connectionProvider.getTable().put(putList);
            }
        }
        for (List<Delete> deleteList : familyDeleteListMap.values()) {
            if (CollectionUtils.isNotEmpty(deleteList)) {
                connectionProvider.getTable().delete(deleteList);
            }
        }
        familyPutListMap.clear();
        familyDeleteListMap.clear();
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
