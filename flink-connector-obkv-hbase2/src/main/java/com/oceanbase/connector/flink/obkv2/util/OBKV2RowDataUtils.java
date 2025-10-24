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

package com.oceanbase.connector.flink.obkv2.util;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;
import java.math.BigDecimal;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision;

/** Utilities for encoding RowData fields to byte arrays for OBKV HBase. */
public class OBKV2RowDataUtils {

    /** Runtime encoder that encodes a specified field in RowData into byte[]. */
    @FunctionalInterface
    public interface FieldEncoder extends Serializable {
        byte[] encode(RowData row, int pos);
    }

    /**
     * Create a field encoder for the given logical type.
     *
     * @param fieldType the logical type of the field
     * @return the field encoder
     */
    public static FieldEncoder createFieldEncoder(LogicalType fieldType) {
        switch (fieldType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return (row, pos) -> Bytes.toBytes(row.getString(pos).toString());
            case BOOLEAN:
                return (row, pos) -> Bytes.toBytes(row.getBoolean(pos));
            case BINARY:
            case VARBINARY:
                return RowData::getBinary;
            case DECIMAL:
                return createDecimalEncoder((DecimalType) fieldType);
            case TINYINT:
                return (row, pos) -> new byte[] {row.getByte(pos)};
            case SMALLINT:
                return (row, pos) -> Bytes.toBytes(row.getShort(pos));
            case INTEGER:
            case DATE:
            case INTERVAL_YEAR_MONTH:
                return (row, pos) -> Bytes.toBytes(row.getInt(pos));
            case TIME_WITHOUT_TIME_ZONE:
                return (row, pos) -> Bytes.toBytes(row.getInt(pos));
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return (row, pos) -> Bytes.toBytes(row.getLong(pos));
            case FLOAT:
                return (row, pos) -> Bytes.toBytes(row.getFloat(pos));
            case DOUBLE:
                return (row, pos) -> Bytes.toBytes(row.getDouble(pos));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return (row, pos) -> {
                    long millisecond =
                            row.getTimestamp(pos, getPrecision(fieldType)).getMillisecond();
                    return Bytes.toBytes(millisecond);
                };
            default:
                throw new UnsupportedOperationException("Unsupported type: " + fieldType);
        }
    }

    private static FieldEncoder createDecimalEncoder(DecimalType decimalType) {
        final int precision = decimalType.getPrecision();
        final int scale = decimalType.getScale();
        return (row, pos) -> {
            BigDecimal decimal = row.getDecimal(pos, precision, scale).toBigDecimal();
            return Bytes.toBytes(decimal);
        };
    }

    /**
     * Parse timestamp value from RowData.
     *
     * @param row the row data
     * @param pos the field position
     * @param fieldType the field type
     * @return the timestamp in milliseconds, or null if the field is null
     */
    public static Long parseTsValueFromRowData(RowData row, int pos, LogicalType fieldType) {
        if (fieldType == null) {
            throw new UnsupportedOperationException("Timestamp column at " + pos + " is null");
        }
        if (row.isNullAt(pos)) {
            return null;
        }
        switch (fieldType.getTypeRoot()) {
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return row.getLong(pos);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return row.getTimestamp(pos, getPrecision(fieldType)).getMillisecond();
            default:
                throw new UnsupportedOperationException("Invalid timestamp type: " + fieldType);
        }
    }
}
