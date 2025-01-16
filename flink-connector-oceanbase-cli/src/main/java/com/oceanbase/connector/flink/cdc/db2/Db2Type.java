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

package com.oceanbase.connector.flink.cdc.db2;

import com.oceanbase.connector.flink.table.OceanBaseType;

import org.apache.flink.util.Preconditions;

public class Db2Type {
    private static final String BOOLEAN = "BOOLEAN";
    private static final String SMALLINT = "SMALLINT";
    private static final String INTEGER = "INTEGER";
    private static final String INT = "INT";
    private static final String BIGINT = "BIGINT";
    private static final String REAL = "REAL";
    private static final String DECFLOAT = "DECFLOAT";
    private static final String DOUBLE = "DOUBLE";
    private static final String DECIMAL = "DECIMAL";
    private static final String NUMERIC = "NUMERIC";
    private static final String DATE = "DATE";
    private static final String TIME = "TIME";
    private static final String TIMESTAMP = "TIMESTAMP";
    private static final String CHARACTER = "CHARACTER";
    private static final String CHAR = "CHAR";
    private static final String LONG_VARCHAR = "LONG VARCHAR";
    private static final String VARCHAR = "VARCHAR";
    private static final String XML = "XML";
    private static final String VARGRAPHIC = "VARGRAPHIC";

    public static String toOceanBaseType(String db2Type, Integer precision, Integer scale) {
        db2Type = db2Type.toUpperCase();
        switch (db2Type) {
            case BOOLEAN:
                return OceanBaseType.BOOLEAN;
            case SMALLINT:
                return OceanBaseType.SMALLINT;
            case INTEGER:
            case INT:
                return OceanBaseType.INT;
            case BIGINT:
                return OceanBaseType.BIGINT;
            case REAL:
                return OceanBaseType.FLOAT;
            case DOUBLE:
                return OceanBaseType.DOUBLE;
            case DATE:
                return OceanBaseType.DATE;
            case DECFLOAT:
            case DECIMAL:
            case NUMERIC:
                if (precision != null && precision > 0 && precision <= 38) {
                    if (scale != null && scale >= 0) {
                        return String.format("%s(%s,%s)", OceanBaseType.DECIMAL, precision, scale);
                    }
                    return String.format("%s(%s,%s)", OceanBaseType.DECIMAL, precision, 0);
                } else {
                    return OceanBaseType.VARCHAR;
                }
            case CHARACTER:
            case CHAR:
            case VARCHAR:
            case LONG_VARCHAR:
                Preconditions.checkNotNull(precision);
                return precision * 3 > 65533
                        ? OceanBaseType.VARCHAR
                        : String.format("%s(%s)", OceanBaseType.VARCHAR, precision * 3);
            case TIMESTAMP:
                return String.format(
                        "%s(%s)", OceanBaseType.TIMESTAMP, Math.min(scale == null ? 0 : scale, 6));
            case TIME:
            case VARGRAPHIC:
                // Currently, the Flink CDC connector does not support the XML data type from DB2.
                // Case XML:
                return OceanBaseType.VARCHAR;
            default:
                throw new UnsupportedOperationException("Unsupported DB2 Type: " + db2Type);
        }
    }
}
