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

package com.oceanbase.connector.flink.table;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryArrayData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.RowKind;

import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.sql.Time;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class OceanBaseRowDataSerializationSchema
        extends AbstractRecordSerializationSchema<RowData> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG =
            LoggerFactory.getLogger(OceanBaseRowDataSerializationSchema.class);

    private final TableInfo tableInfo;
    private final RowData.FieldGetter[] fieldGetters;
    private final SerializationRuntimeConverter[] fieldConverters;

    public OceanBaseRowDataSerializationSchema(TableInfo tableInfo) {
        this.tableInfo = tableInfo;
        this.fieldGetters =
                IntStream.range(0, tableInfo.getDataTypes().size())
                        .boxed()
                        .map(i -> RowData.createFieldGetter(tableInfo.getDataTypes().get(i), i))
                        .toArray(RowData.FieldGetter[]::new);
        this.fieldConverters =
                tableInfo.getDataTypes().stream()
                        .map(this::getOrCreateConverter)
                        .toArray(SerializationRuntimeConverter[]::new);
    }

    @Override
    public Record serialize(RowData rowData) {
        Object[] values = new Object[fieldGetters.length];
        for (int i = 0; i < fieldGetters.length; i++) {
            values[i] = fieldConverters[i].convert(fieldGetters[i].getFieldOrNull(rowData));
        }
        return new DataChangeRecord(
                tableInfo,
                (rowData.getRowKind() == RowKind.INSERT
                                || rowData.getRowKind() == RowKind.UPDATE_AFTER)
                        ? DataChangeRecord.Type.UPSERT
                        : DataChangeRecord.Type.DELETE,
                values);
    }

    @Override
    protected SerializationRuntimeConverter createNotNullConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
            case BIGINT:
            case INTERVAL_DAY_TIME:
            case FLOAT:
            case DOUBLE:
            case BINARY:
            case VARBINARY:
                return data -> data;
            case CHAR:
            case VARCHAR:
                return Object::toString;
            case DATE:
                return data -> Date.valueOf(LocalDate.ofEpochDay((int) data));
            case TIME_WITHOUT_TIME_ZONE:
                return data -> Time.valueOf(LocalTime.ofNanoOfDay((int) data * 1_000_000L));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return data -> ((TimestampData) data).toTimestamp();
            case DECIMAL:
                return data -> ((DecimalData) data).toBigDecimal();
            case ARRAY:
                return data -> {
                    int depth = checkArrayDataNestDepth(type);
                    if (depth > 6) {
                        LOG.warn("Array nesting depth exceeds maximum allowed level of 6.");
                    }
                    return parseArrayData((BinaryArrayData) data, type);
                };
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    public int checkArrayDataNestDepth(LogicalType type) {
        LogicalType lt = ((ArrayType) type).getElementType();
        int depth = 1;
        while (LogicalTypeRoot.ARRAY.equals(lt.getTypeRoot())) {
            lt = ((ArrayType) lt).getElementType();
            depth++;
        }
        return depth;
    }

    public static String parseArrayData(BinaryArrayData arrayData, LogicalType type) {
        StringBuilder result = new StringBuilder();

        result.append("[");
        LogicalType lt = ((ArrayType) type).getElementType();
        List<Object> data = Lists.newArrayList((arrayData).toObjectArray(lt));
        if (LogicalTypeRoot.ARRAY.equals(lt.getTypeRoot())) {
            // traversal of the nested array
            for (int i = 0; i < data.size(); i++) {
                if (i > 0) {
                    result.append(",");
                }
                String parsed = parseArrayData((BinaryArrayData) data.get(i), lt);
                result.append(parsed);
            }
        } else {
            String dataStr =
                    data.stream()
                            .map(
                                    element -> {
                                        if (element instanceof Integer) {
                                            return element.toString();
                                        } else if (element instanceof Boolean) {
                                            return element.toString();
                                        } else if (element instanceof Long) {
                                            return element.toString();
                                        } else if (element instanceof Short) {
                                            return element.toString();
                                        } else if (element instanceof Float) {
                                            return element.toString();
                                        } else if (element instanceof Double) {
                                            return element.toString();
                                        } else if (element instanceof Byte) {
                                            return element.toString();
                                        } else {
                                            // Handle other types as needed
                                            return element.toString();
                                        }
                                    })
                            .collect(Collectors.joining(","));
            result.append(dataStr);
        }
        result.append("]");
        return result.toString();
    }
}
