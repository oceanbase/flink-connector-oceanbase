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

package com.oceanbase.connector.flink.converter;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampType;

import java.sql.Date;
import java.sql.Time;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class OceanBaseRowConverter extends AbstractRowConverter {

    private static final long serialVersionUID = 1L;

    @Override
    protected FieldConverter createExternalConverter(LogicalType type) {
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
