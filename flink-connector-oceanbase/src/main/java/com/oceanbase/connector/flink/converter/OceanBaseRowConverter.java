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

package com.oceanbase.connector.flink.converter;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.TimestampType;

import java.io.Serializable;
import java.sql.Date;
import java.sql.Time;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class OceanBaseRowConverter implements Serializable {

    public static RowData.FieldGetter createFieldGetter(LogicalType type, int fieldIndex) {
        return row -> createNullableExternalConverter(type).toExternal(row, fieldIndex);
    }

    public interface FieldConverter extends Serializable {
        Object toExternal(RowData rowData, int fieldIndex);
    }

    public static FieldConverter createNullableExternalConverter(LogicalType type) {
        return wrapIntoNullableExternalConverter(createExternalConverter(type), type);
    }

    public static FieldConverter wrapIntoNullableExternalConverter(
            FieldConverter fieldConverter, LogicalType type) {
        return (val, fieldIndex) -> {
            if (val == null
                    || val.isNullAt(fieldIndex)
                    || LogicalTypeRoot.NULL.equals(type.getTypeRoot())) {
                return null;
            } else {
                return fieldConverter.toExternal(val, fieldIndex);
            }
        };
    }

    public static FieldConverter createExternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return RowData::getBoolean;
            case TINYINT:
                return RowData::getByte;
            case SMALLINT:
                return RowData::getShort;
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return RowData::getInt;
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return RowData::getLong;
            case FLOAT:
                return RowData::getFloat;
            case DOUBLE:
                return RowData::getDouble;
            case CHAR:
            case VARCHAR:
                return (rowData, fieldIndex) -> rowData.getString(fieldIndex).toString();
            case BINARY:
            case VARBINARY:
                return RowData::getBinary;
            case DATE:
                return (rowData, fieldIndex) ->
                        Date.valueOf(LocalDate.ofEpochDay(rowData.getInt(fieldIndex)));
            case TIME_WITHOUT_TIME_ZONE:
                return (rowData, fieldIndex) ->
                        Time.valueOf(
                                LocalTime.ofNanoOfDay(rowData.getInt(fieldIndex) * 1_000_000L));
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final int timestampPrecision = ((TimestampType) type).getPrecision();
                return (rowData, fieldIndex) ->
                        rowData.getTimestamp(fieldIndex, timestampPrecision).toTimestamp();
            case DECIMAL:
                final int decimalPrecision = ((DecimalType) type).getPrecision();
                final int decimalScale = ((DecimalType) type).getScale();
                return (rowData, fieldIndex) ->
                        rowData.getDecimal(fieldIndex, decimalPrecision, decimalScale)
                                .toBigDecimal();
            case ARRAY:
                return (rowData, fieldIndex) -> {
                    ArrayData arrayData = rowData.getArray(fieldIndex);
                    return IntStream.range(0, arrayData.size())
                            .mapToObj(i -> arrayData.getString(i).toString())
                            .collect(Collectors.joining(","));
                };
            case MAP:
            case MULTISET:
            case ROW:
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }
}
