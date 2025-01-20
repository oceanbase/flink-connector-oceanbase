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
import org.apache.flink.table.types.logical.LogicalType;

import java.sql.Types;

public class OceanBaseTypeMapper {

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
}
