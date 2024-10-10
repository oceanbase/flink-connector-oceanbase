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

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;

import java.sql.Date;
import java.sql.Time;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class OceanBaseRowDataSerializationSchema
        extends AbstractRecordSerializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

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
                    ArrayData arrayData = (ArrayData) data;
                    return IntStream.range(0, arrayData.size())
                            .mapToObj(i -> arrayData.getString(i).toString())
                            .collect(Collectors.joining(","));
                };
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }
}
