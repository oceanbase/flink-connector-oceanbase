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

package com.oceanbase.connector.flink.tools.cdc.postgres;

import com.oceanbase.connector.flink.tools.catalog.OceanBaseType;

import org.apache.flink.util.Preconditions;

public class PostgresType {
    private static final String INT2 = "int2";
    private static final String SMALLSERIAL = "smallserial";
    private static final String INT4 = "int4";
    private static final String SERIAL = "serial";
    private static final String INT8 = "int8";
    private static final String BIGSERIAL = "bigserial";
    private static final String NUMERIC = "numeric";
    private static final String FLOAT4 = "float4";
    private static final String FLOAT8 = "float8";
    private static final String BPCHAR = "bpchar";
    private static final String TIMESTAMP = "timestamp";
    private static final String TIMESTAMPTZ = "timestamptz";
    private static final String DATE = "date";
    private static final String BOOL = "bool";
    private static final String BIT = "bit";
    private static final String POINT = "point";
    private static final String LINE = "line";
    private static final String LSEG = "lseg";
    private static final String BOX = "box";
    private static final String PATH = "path";
    private static final String POLYGON = "polygon";
    private static final String CIRCLE = "circle";
    private static final String VARCHAR = "varchar";
    private static final String TEXT = "text";
    private static final String TIME = "time";
    private static final String TIMETZ = "timetz";
    private static final String INTERVAL = "interval";
    private static final String CIDR = "cidr";
    private static final String INET = "inet";
    private static final String MACADDR = "macaddr";
    private static final String VARBIT = "varbit";
    private static final String UUID = "uuid";
    private static final String BYTEA = "bytea";
    private static final String JSON = "json";
    private static final String JSONB = "jsonb";
    private static final String _INT2 = "_int2";
    private static final String _INT4 = "_int4";
    private static final String _INT8 = "_int8";
    private static final String _FLOAT4 = "_float4";
    private static final String _FLOAT8 = "_float8";
    private static final String _DATE = "_date";
    private static final String _TIMESTAMP = "_timestamp";
    private static final String _BOOL = "_bool";
    private static final String _TEXT = "_text";

    public static String toOceanBaseType(String postgresType, Integer precision, Integer scale) {
        postgresType = postgresType.toLowerCase();
        if (postgresType.startsWith("_")) {
            return OceanBaseType.VARCHAR;
        }
        switch (postgresType) {
            case INT2:
            case SMALLSERIAL:
                return OceanBaseType.TINYINT;
            case INT4:
            case SERIAL:
                return OceanBaseType.INT;
            case INT8:
            case BIGSERIAL:
                return OceanBaseType.BIGINT;
            case NUMERIC:
                return precision != null && precision > 0 && precision <= 38
                        ? String.format(
                                "%s(%s,%s)",
                                OceanBaseType.DECIMAL,
                                precision,
                                scale != null && scale >= 0 ? scale : 0)
                        : OceanBaseType.VARCHAR;
            case FLOAT4:
                return OceanBaseType.FLOAT;
            case FLOAT8:
                return OceanBaseType.DOUBLE;
            case TIMESTAMP:
            case TIMESTAMPTZ:
                return String.format(
                        "%s(%s)", OceanBaseType.TIMESTAMP, Math.min(scale == null ? 0 : scale, 6));
            case DATE:
                return OceanBaseType.DATE;
            case BOOL:
                return OceanBaseType.BOOLEAN;
            case BIT:
                return precision == 1 ? OceanBaseType.BOOLEAN : OceanBaseType.VARCHAR;
            case BPCHAR:
            case VARCHAR:
                Preconditions.checkNotNull(precision);
                return precision * 3 > 65533
                        ? OceanBaseType.VARCHAR
                        : String.format("%s(%s)", OceanBaseType.VARCHAR, precision * 3);
            case POINT:
            case LINE:
            case LSEG:
            case BOX:
            case PATH:
            case POLYGON:
            case CIRCLE:
            case TEXT:
            case TIME:
            case TIMETZ:
            case INTERVAL:
            case CIDR:
            case INET:
            case MACADDR:
            case VARBIT:
            case UUID:
            case BYTEA:
                return OceanBaseType.VARCHAR;
            case JSON:
            case JSONB:
                return OceanBaseType.JSONB;
                /* Compatible with oceanbase1.2 array type can only be used in dup table,
                   and then converted to array in the next version
                case _BOOL:
                    return String.format("%s<%s>", OceanBaseType.ARRAY, OceanBaseType.BOOLEAN);
                case _INT2:
                    return String.format("%s<%s>", OceanBaseType.ARRAY, OceanBaseType.TINYINT);
                case _INT4:
                    return String.format("%s<%s>", OceanBaseType.ARRAY, OceanBaseType.INT);
                case _INT8:
                    return String.format("%s<%s>", OceanBaseType.ARRAY, OceanBaseType.BIGINT);
                case _FLOAT4:
                    return String.format("%s<%s>", OceanBaseType.ARRAY, OceanBaseType.FLOAT);
                case _FLOAT8:
                    return String.format("%s<%s>", OceanBaseType.ARRAY, OceanBaseType.DOUBLE);
                case _TEXT:
                    return String.format("%s<%s>", OceanBaseType.ARRAY, OceanBaseType.STRING);
                case _DATE:
                    return String.format("%s<%s>", OceanBaseType.ARRAY, OceanBaseType.DATE_V2);
                case _TIMESTAMP:
                    return String.format("%s<%s>", OceanBaseType.ARRAY, OceanBaseType.DATETIME_V2);
                **/
            default:
                throw new UnsupportedOperationException(
                        "Unsupported Postgres Type: " + postgresType);
        }
    }
}
