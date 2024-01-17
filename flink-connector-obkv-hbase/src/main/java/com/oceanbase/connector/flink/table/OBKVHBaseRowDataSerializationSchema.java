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

package com.oceanbase.connector.flink.table;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;

import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashMap;
import java.util.Map;

public class OBKVHBaseRowDataSerializationSchema
        extends AbstractRecordSerializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private final HTableInfo tableInfo;
    private final RowData.FieldGetter rowKeyGetter;
    private final SerializationRuntimeConverter rowKeyConverter;
    private final Map<String, RowData.FieldGetter[]> columnGetterMap;
    private final Map<String, SerializationRuntimeConverter[]> columnConverterMap;

    public OBKVHBaseRowDataSerializationSchema(HTableInfo tableInfo) {
        this.tableInfo = tableInfo;
        this.rowKeyGetter =
                RowData.createFieldGetter(
                        tableInfo.getRowKeyType(),
                        tableInfo.getFieldIndex(tableInfo.getRowKeyName()));
        this.rowKeyConverter = getOrCreateConverter(tableInfo.getRowKeyType());
        this.columnGetterMap = new HashMap<>();
        this.columnConverterMap = new HashMap<>();
        for (String familyName : tableInfo.getFamilyNames()) {
            String[] columnNames = tableInfo.getColumnNames(familyName);
            LogicalType[] columnTypes = tableInfo.getColumnTypes(familyName);
            RowData.FieldGetter[] columnGetters = new RowData.FieldGetter[columnNames.length];
            SerializationRuntimeConverter[] columnConverters =
                    new SerializationRuntimeConverter[columnNames.length];
            for (int i = 0; i < columnNames.length; i++) {
                columnGetters[i] = RowData.createFieldGetter(columnTypes[i], i);
                columnConverters[i] = getOrCreateConverter(columnTypes[i]);
            }
            columnGetterMap.put(familyName, columnGetters);
            columnConverterMap.put(familyName, columnConverters);
        }
    }

    @Override
    public Record serialize(RowData rowData) {
        Object rowKey = rowKeyConverter.convert(rowKeyGetter.getFieldOrNull(rowData));
        Object[] values = new Object[tableInfo.getFamilyNames().size() + 1];
        values[tableInfo.getFieldIndex(tableInfo.getRowKeyName())] = rowKey;
        for (String familyName : tableInfo.getFamilyNames()) {
            values[tableInfo.getFieldIndex(familyName)] = getFamilyValue(rowData, familyName);
        }
        return new DataChangeRecord(
                tableInfo,
                (rowData.getRowKind() == RowKind.INSERT
                                || rowData.getRowKind() == RowKind.UPDATE_AFTER)
                        ? DataChangeRecord.Type.UPSERT
                        : DataChangeRecord.Type.DELETE,
                values);
    }

    private Map<String, Object> getFamilyValue(RowData rowData, String familyName) {
        int familyIndex = tableInfo.getFieldIndex(familyName);
        String[] columnNames = tableInfo.getColumnNames(familyName);
        RowData family = rowData.getRow(familyIndex, columnNames.length);
        if (family == null) {
            return null;
        }
        Map<String, Object> value = new HashMap<>();
        for (int i = 0; i < columnNames.length; i++) {
            value.put(
                    columnNames[i],
                    columnConverterMap.get(familyName)[i].convert(
                            columnGetterMap.get(familyName)[i].getFieldOrNull(family)));
        }
        return value;
    }

    @Override
    protected SerializationRuntimeConverter createNotNullConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return data -> Bytes.toBytes((Boolean) data);
            case TINYINT:
                return data -> new byte[] {(byte) data};
            case SMALLINT:
                return data -> Bytes.toBytes((short) data);
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case INTERVAL_YEAR_MONTH:
                return data -> Bytes.toBytes((int) data);
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return data -> Bytes.toBytes((long) data);
            case FLOAT:
                return data -> Bytes.toBytes((float) data);
            case DOUBLE:
                return data -> Bytes.toBytes((double) data);
            case CHAR:
            case VARCHAR:
                return data -> Bytes.toBytes(data.toString());
            case BINARY:
            case VARBINARY:
                return data -> data;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return data -> Bytes.toBytes(((TimestampData) data).getMillisecond());
            case DECIMAL:
                return data -> Bytes.toBytes(((DecimalData) data).toBigDecimal());
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }
}
