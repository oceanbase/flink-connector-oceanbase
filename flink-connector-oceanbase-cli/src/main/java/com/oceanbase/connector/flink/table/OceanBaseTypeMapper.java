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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.utils.LogicalTypeDefaultVisitor;

import java.sql.Types;

import static com.oceanbase.connector.flink.table.OceanBaseType.BIGINT;
import static com.oceanbase.connector.flink.table.OceanBaseType.BOOLEAN;
import static com.oceanbase.connector.flink.table.OceanBaseType.DOUBLE;
import static com.oceanbase.connector.flink.table.OceanBaseType.FLOAT;
import static com.oceanbase.connector.flink.table.OceanBaseType.INT;
import static com.oceanbase.connector.flink.table.OceanBaseType.SMALLINT;
import static com.oceanbase.connector.flink.table.OceanBaseType.TINYINT;

public class OceanBaseTypeMapper {

    /** Max size of char type of OceanBase. */
    public static final int MAX_CHAR_SIZE = 256;

    /** Max size of varchar type of OceanBase. */
    public static final int MAX_VARCHAR_SIZE = 262144;
    /* Max precision of datetime type of OceanBase. */
    public static final int MAX_SUPPORTED_DATE_TIME_PRECISION = 6;

    public static LogicalType convertToLogicalType(int jdbcType, int precision, int scale) {
        switch (jdbcType) {
            case Types.INTEGER:
                return DataTypes.INT().getLogicalType();
            case Types.BIGINT:
                return DataTypes.BIGINT().getLogicalType();
            case Types.DOUBLE:
                return DataTypes.DOUBLE().getLogicalType();
            case Types.FLOAT:
            case Types.REAL:
                return DataTypes.FLOAT().getLogicalType();
            case Types.LONGVARCHAR:
            case Types.VARCHAR:
                if (precision > 0) {
                    return DataTypes.STRING().getLogicalType();
                } else {
                    return DataTypes.STRING().getLogicalType();
                }
            case Types.CHAR:
                if (precision > 0) {
                    return DataTypes.CHAR(precision).getLogicalType();
                } else {
                    return DataTypes.STRING().getLogicalType();
                }
            case Types.TIMESTAMP:
                if (precision > 0 && precision <= 3) {
                    return DataTypes.TIMESTAMP(precision).getLogicalType();
                } else {
                    return DataTypes.TIMESTAMP(3).getLogicalType();
                }
            case Types.DATE:
                return DataTypes.DATE().getLogicalType();
            case Types.TIME:
                if (precision > 0 && precision <= 3) {
                    return DataTypes.TIME(precision).getLogicalType();
                } else {
                    return DataTypes.TIME(0).getLogicalType();
                }
            case Types.DECIMAL:
            case Types.NUMERIC:
                if (precision > 0 && precision <= 38 && scale >= 0) {
                    return DataTypes.DECIMAL(precision, scale).getLogicalType();
                } else {
                    return DataTypes.DECIMAL(10, 0).getLogicalType();
                }
            case Types.BOOLEAN:
            case Types.BIT:
                return DataTypes.BOOLEAN().getLogicalType();
            case Types.TINYINT:
                return DataTypes.TINYINT().getLogicalType();
            case Types.SMALLINT:
                return DataTypes.SMALLINT().getLogicalType();
            case Types.BLOB:
                return DataTypes.BYTES().getLogicalType();
            case Types.CLOB:
                return DataTypes.STRING().getLogicalType();
            case Types.BINARY:
                if (precision > 0) {
                    return DataTypes.BINARY(precision).getLogicalType();
                } else {
                    return DataTypes.BYTES().getLogicalType();
                }
            case Types.LONGVARBINARY:
            case Types.VARBINARY:
                if (precision > 0) {
                    return DataTypes.VARBINARY(precision).getLogicalType();
                } else {
                    return DataTypes.BYTES().getLogicalType();
                }
            default:
                throw new IllegalArgumentException("Unsupported JDBC type: " + jdbcType);
        }
    }

    public static String toOceanBaseType(DataType flinkType) {
        LogicalType logicalType = flinkType.getLogicalType();
        return logicalType.accept(new LogicalTypeVisitor(logicalType));
    }

    private static class LogicalTypeVisitor extends LogicalTypeDefaultVisitor<String> {
        private final LogicalType type;

        LogicalTypeVisitor(LogicalType type) {
            this.type = type;
        }

        @Override
        public String visit(CharType charType) {
            long length = charType.getLength() * 3L;
            if (length <= MAX_CHAR_SIZE) {
                return String.format("%s(%s)", OceanBaseType.CHAR, length);
            } else {
                return visit(new VarCharType(charType.getLength()));
            }
        }

        @Override
        public String visit(BooleanType booleanType) {
            return BOOLEAN;
        }

        @Override
        public String visit(TinyIntType tinyIntType) {
            return TINYINT;
        }

        @Override
        public String visit(SmallIntType smallIntType) {
            return SMALLINT;
        }

        @Override
        public String visit(IntType intType) {
            return INT;
        }

        @Override
        public String visit(BigIntType bigIntType) {
            return BIGINT;
        }

        @Override
        public String visit(FloatType floatType) {
            return FLOAT;
        }

        @Override
        public String visit(DoubleType doubleType) {
            return DOUBLE;
        }

        @Override
        protected String defaultMethod(LogicalType logicalType) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Flink doesn't support converting type %s to OceanBase type yet.",
                            type.toString()));
        }
    }
}
