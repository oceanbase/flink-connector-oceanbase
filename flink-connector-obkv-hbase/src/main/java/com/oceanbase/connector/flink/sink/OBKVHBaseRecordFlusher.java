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

import com.oceanbase.connector.flink.OBKVHBaseConnectorOptions;
import com.oceanbase.connector.flink.connection.OBKVHBaseConnectionProvider;
import com.oceanbase.connector.flink.table.DataChangeRecord;
import com.oceanbase.connector.flink.table.HTableInfo;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class OBKVHBaseRecordFlusher implements RecordFlusher {

    private static final long serialVersionUID = 1L;

    private final OBKVHBaseConnectorOptions options;
    private final OBKVHBaseConnectionProvider connectionProvider;

    public OBKVHBaseRecordFlusher(OBKVHBaseConnectorOptions options) {
        this(options, new OBKVHBaseConnectionProvider(options));
    }

    public OBKVHBaseRecordFlusher(
            OBKVHBaseConnectorOptions options, OBKVHBaseConnectionProvider connectionProvider) {
        this.options = options;
        this.connectionProvider = connectionProvider;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void flush(List<DataChangeRecord> records) throws Exception {
        if (records == null || records.isEmpty()) {
            return;
        }

        HTableInfo tableInfo = (HTableInfo) records.get(0).getTable();

        Map<byte[], List<Put>> familyPutListMap = new HashMap<>();
        Map<byte[], List<Delete>> familyDeleteListMap = new HashMap<>();

        for (DataChangeRecord record : records) {
            byte[] rowKey = (byte[]) record.getFieldValue(tableInfo.getRowKeyName());
            for (String familyName : tableInfo.getFamilyNames()) {
                Object familyValue = record.getFieldValue(familyName);
                if (familyValue == null) {
                    continue;
                }
                List<Map.Entry<String, Object>> columnValues =
                        ((Map<String, Object>) familyValue)
                                .entrySet().stream()
                                        .filter(entry -> entry.getValue() != null)
                                        .collect(Collectors.toList());
                if (columnValues.isEmpty()) {
                    continue;
                }
                byte[] family = Bytes.toBytes(familyName);
                if (record.isUpsert()) {
                    Put put = new Put(rowKey);
                    columnValues.forEach(
                            entry ->
                                    put.addColumn(
                                            family,
                                            Bytes.toBytes(entry.getKey()),
                                            (byte[]) entry.getValue()));
                    familyPutListMap.computeIfAbsent(family, k -> new ArrayList<>()).add(put);
                } else {
                    Delete delete = new Delete(rowKey);
                    for (Map.Entry<String, Object> entry :
                            ((Map<String, Object>) familyValue).entrySet()) {
                        delete.addColumn(family, Bytes.toBytes(entry.getKey()));
                    }
                    columnValues.forEach(
                            entry -> delete.addColumn(family, Bytes.toBytes(entry.getKey())));
                    familyDeleteListMap.computeIfAbsent(family, k -> new ArrayList<>()).add(delete);
                }
            }
        }

        flush(
                connectionProvider.getHTableClient(tableInfo.getTableId()),
                familyPutListMap,
                familyDeleteListMap);
    }

    private void flush(
            Table table,
            Map<byte[], List<Put>> familyPutListMap,
            Map<byte[], List<Delete>> familyDeleteListMap)
            throws Exception {
        for (List<Put> putList : familyPutListMap.values()) {
            if (CollectionUtils.isNotEmpty(putList)) {
                table.put(putList);
            }
        }
        for (List<Delete> deleteList : familyDeleteListMap.values()) {
            if (CollectionUtils.isNotEmpty(deleteList)) {
                table.delete(deleteList);
            }
        }
    }

    @Override
    public void close() throws Exception {
        connectionProvider.close();
    }
}
