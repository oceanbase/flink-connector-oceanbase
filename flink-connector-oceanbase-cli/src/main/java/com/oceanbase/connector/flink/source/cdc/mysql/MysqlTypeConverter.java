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
package com.oceanbase.connector.flink.source.cdc.mysql;

import com.oceanbase.connector.flink.table.OceanBaseType;

public class MysqlTypeConverter {
    private static final String BIT = "BIT";
    private static final String BOOLEAN = "BOOLEAN";
    private static final String BOOL = "BOOL";
    private static final String TINYINT = "TINYINT";
    private static final String TINYINT_UNSIGNED = "TINYINT UNSIGNED";
    private static final String TINYINT_UNSIGNED_ZEROFILL = "TINYINT UNSIGNED ZEROFILL";
    private static final String SMALLINT = "SMALLINT";
    private static final String SMALLINT_UNSIGNED = "SMALLINT UNSIGNED";
    private static final String SMALLINT_UNSIGNED_ZEROFILL = "SMALLINT UNSIGNED ZEROFILL";
    private static final String MEDIUMINT = "MEDIUMINT";
    private static final String MEDIUMINT_UNSIGNED = "MEDIUMINT UNSIGNED";
    private static final String MEDIUMINT_UNSIGNED_ZEROFILL = "MEDIUMINT UNSIGNED ZEROFILL";
    private static final String INT = "INT";
    private static final String INT_UNSIGNED = "INT UNSIGNED";
    private static final String INT_UNSIGNED_ZEROFILL = "INT UNSIGNED ZEROFILL";
    private static final String INTEGER = "INTEGER";
    private static final String INTEGER_UNSIGNED = "INTEGER UNSIGNED";
    private static final String INTEGER_UNSIGNED_ZEROFILL = "INTEGER UNSIGNED ZEROFILL";
    private static final String BIGINT = "BIGINT";
    private static final String SERIAL = "SERIAL";
    private static final String BIGINT_UNSIGNED = "BIGINT UNSIGNED";
    private static final String BIGINT_UNSIGNED_ZEROFILL = "BIGINT UNSIGNED ZEROFILL";
    private static final String REAL = "REAL";
    private static final String REAL_UNSIGNED = "REAL UNSIGNED";
    private static final String REAL_UNSIGNED_ZEROFILL = "REAL UNSIGNED ZEROFILL";
    private static final String FLOAT = "FLOAT";
    private static final String FLOAT_UNSIGNED = "FLOAT UNSIGNED";
    private static final String FLOAT_UNSIGNED_ZEROFILL = "FLOAT UNSIGNED ZEROFILL";
    private static final String DOUBLE = "DOUBLE";
    private static final String DOUBLE_UNSIGNED = "DOUBLE UNSIGNED";
    private static final String DOUBLE_UNSIGNED_ZEROFILL = "DOUBLE UNSIGNED ZEROFILL";
    private static final String DOUBLE_PRECISION = "DOUBLE PRECISION";
    private static final String DOUBLE_PRECISION_UNSIGNED = "DOUBLE PRECISION UNSIGNED";
    private static final String DOUBLE_PRECISION_UNSIGNED_ZEROFILL =
            "DOUBLE PRECISION UNSIGNED ZEROFILL";
    private static final String NUMERIC = "NUMERIC";
    private static final String NUMERIC_UNSIGNED = "NUMERIC UNSIGNED";
    private static final String NUMERIC_UNSIGNED_ZEROFILL = "NUMERIC UNSIGNED ZEROFILL";
    private static final String FIXED = "FIXED";
    private static final String FIXED_UNSIGNED = "FIXED UNSIGNED";
    private static final String FIXED_UNSIGNED_ZEROFILL = "FIXED UNSIGNED ZEROFILL";
    private static final String DECIMAL = "DECIMAL";
    private static final String DECIMAL_UNSIGNED = "DECIMAL UNSIGNED";
    private static final String DECIMAL_UNSIGNED_ZEROFILL = "DECIMAL UNSIGNED ZEROFILL";
    private static final String CHAR = "CHAR";
    private static final String VARCHAR = "VARCHAR";
    private static final String TINYTEXT = "TINYTEXT";
    private static final String MEDIUMTEXT = "MEDIUMTEXT";
    private static final String TEXT = "TEXT";
    private static final String LONGTEXT = "LONGTEXT";
    private static final String DATE = "DATE";
    private static final String TIME = "TIME";
    private static final String DATETIME = "DATETIME";
    private static final String TIMESTAMP = "TIMESTAMP";
    private static final String YEAR = "YEAR";
    private static final String BINARY = "BINARY";
    private static final String VARBINARY = "VARBINARY";
    private static final String TINYBLOB = "TINYBLOB";
    private static final String MEDIUMBLOB = "MEDIUMBLOB";
    private static final String BLOB = "BLOB";
    private static final String LONGBLOB = "LONGBLOB";
    private static final String JSON = "JSON";
    private static final String ENUM = "ENUM";
    private static final String SET = "SET";

    public static String toOceanBaseType(String type, Integer length, Integer scale) {
        switch (type.toUpperCase()) {
            case BIT:
                return OceanBaseType.BIT;
            case BOOLEAN:
            case BOOL:
                return OceanBaseType.BOOLEAN;
            case TINYINT:
            case TINYINT_UNSIGNED:
            case TINYINT_UNSIGNED_ZEROFILL:
                return OceanBaseType.TINYINT;
            case SMALLINT:
            case SMALLINT_UNSIGNED:
            case SMALLINT_UNSIGNED_ZEROFILL:
                return OceanBaseType.SMALLINT;
            case MEDIUMINT:
            case MEDIUMINT_UNSIGNED:
            case MEDIUMINT_UNSIGNED_ZEROFILL:
                return OceanBaseType.MEDIUMINT;
            case INT:
            case INTEGER:
            case INT_UNSIGNED:
            case INT_UNSIGNED_ZEROFILL:
            case INTEGER_UNSIGNED:
            case INTEGER_UNSIGNED_ZEROFILL:
                return OceanBaseType.INT;
            case BIGINT:
            case BIGINT_UNSIGNED:
            case BIGINT_UNSIGNED_ZEROFILL:
            case SERIAL:
                return OceanBaseType.BIGINT;
            case REAL:
            case REAL_UNSIGNED:
            case REAL_UNSIGNED_ZEROFILL:
            case FLOAT:
            case FLOAT_UNSIGNED:
            case FLOAT_UNSIGNED_ZEROFILL:
                return OceanBaseType.FLOAT;
            case DOUBLE:
            case DOUBLE_UNSIGNED:
            case DOUBLE_UNSIGNED_ZEROFILL:
            case DOUBLE_PRECISION:
            case DOUBLE_PRECISION_UNSIGNED:
            case DOUBLE_PRECISION_UNSIGNED_ZEROFILL:
                return OceanBaseType.DOUBLE;
            case NUMERIC:
            case NUMERIC_UNSIGNED:
            case NUMERIC_UNSIGNED_ZEROFILL:
            case FIXED:
            case FIXED_UNSIGNED:
            case FIXED_UNSIGNED_ZEROFILL:
            case DECIMAL:
            case DECIMAL_UNSIGNED:
            case DECIMAL_UNSIGNED_ZEROFILL:
                return OceanBaseType.DECIMAL + "(" + length + "," + scale + ")";
            case CHAR:
                return OceanBaseType.CHAR + "(" + length + ")";
            case VARCHAR:
                return OceanBaseType.VARCHAR + "(" + length + ")";
            case TINYTEXT:
            case MEDIUMTEXT:
            case TEXT:
            case LONGTEXT:
                return OceanBaseType.TEXT;
            case DATE:
                return OceanBaseType.DATE;
            case TIME:
                return OceanBaseType.TIME + "(" + scale + ")";
            case DATETIME:
                return OceanBaseType.DATETIME + "(" + scale + ")";
            case TIMESTAMP:
                return OceanBaseType.TIMESTAMP + "(" + scale + ")";
            case YEAR:
                return OceanBaseType.YEAR;
            case BINARY:
                return OceanBaseType.BINARY + "(" + length + ")";
            case VARBINARY:
                return OceanBaseType.VARBINARY + "(" + length + ")";
            case TINYBLOB:
            case MEDIUMBLOB:
            case BLOB:
            case LONGBLOB:
                return OceanBaseType.BLOB;
            case JSON:
                return OceanBaseType.JSON;
            case ENUM:
                return OceanBaseType.ENUM;
            case SET:
                return OceanBaseType.SET;
            default:
                throw new UnsupportedOperationException("Unsupported MySQL type: " + type);
        }
    }
}
