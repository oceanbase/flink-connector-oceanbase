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

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.hadoop.hbase.util.Bytes;

import java.math.BigDecimal;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision;

public class OBKVHBaseRowConverter extends AbstractRowConverter {

    private static final long serialVersionUID = 1L;

    private static final int MIN_TIMESTAMP_PRECISION = 0;
    private static final int MAX_TIMESTAMP_PRECISION = 3;
    private static final int MIN_TIME_PRECISION = 0;
    private static final int MAX_TIME_PRECISION = 3;

    @Override
    protected FieldConverter createExternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return (row, fieldIndex) -> Bytes.toBytes(row.getBoolean(fieldIndex));
            case TINYINT:
                return (row, fieldIndex) -> new byte[] {row.getByte(fieldIndex)};
            case SMALLINT:
                return (row, fieldIndex) -> Bytes.toBytes(row.getShort(fieldIndex));
            case INTEGER:
            case DATE:
            case INTERVAL_YEAR_MONTH:
                return (row, fieldIndex) -> Bytes.toBytes(row.getInt(fieldIndex));
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return (row, fieldIndex) -> Bytes.toBytes(row.getLong(fieldIndex));
            case FLOAT:
                return (row, fieldIndex) -> Bytes.toBytes(row.getFloat(fieldIndex));
            case DOUBLE:
                return (row, fieldIndex) -> Bytes.toBytes(row.getDouble(fieldIndex));
            case CHAR:
            case VARCHAR:
                return (row, fieldIndex) -> row.getString(fieldIndex).toBytes();
            case BINARY:
            case VARBINARY:
                return RowData::getBinary;
            case TIME_WITHOUT_TIME_ZONE:
                final int timePrecision = getPrecision(type);
                if (timePrecision < MIN_TIME_PRECISION || timePrecision > MAX_TIME_PRECISION) {
                    throw new UnsupportedOperationException(
                            String.format(
                                    "The precision %s of TIME type is out of the range [%s, %s] supported by "
                                            + "HBase connector",
                                    timePrecision, MIN_TIME_PRECISION, MAX_TIME_PRECISION));
                }
                return (row, fieldIndex) -> Bytes.toBytes(row.getInt(fieldIndex));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int timestampPrecision = getPrecision(type);
                if (timestampPrecision < MIN_TIMESTAMP_PRECISION
                        || timestampPrecision > MAX_TIMESTAMP_PRECISION) {
                    throw new UnsupportedOperationException(
                            String.format(
                                    "The precision %s of TIMESTAMP type is out of the range [%s, %s] supported by "
                                            + "HBase connector",
                                    timestampPrecision,
                                    MIN_TIMESTAMP_PRECISION,
                                    MAX_TIMESTAMP_PRECISION));
                }
                return (row, fieldIndex) -> {
                    long millisecond =
                            row.getTimestamp(fieldIndex, timestampPrecision).getMillisecond();
                    return Bytes.toBytes(millisecond);
                };
            case DECIMAL:
                final int decimalPrecision = ((DecimalType) type).getPrecision();
                final int decimalScale = ((DecimalType) type).getScale();
                return (row, fieldIndex) -> {
                    BigDecimal decimal =
                            row.getDecimal(fieldIndex, decimalPrecision, decimalScale)
                                    .toBigDecimal();
                    return Bytes.toBytes(decimal);
                };
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }
}
