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

import com.oceanbase.connector.flink.dialect.OceanBaseDialect;
import com.oceanbase.connector.flink.dialect.OceanBaseMySQLDialect;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalType;

import java.sql.Date;
import java.sql.Time;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class OceanBaseTestDataSerializationSchema
        extends AbstractRecordSerializationSchema<OceanBaseTestData> {

    private final OceanBaseDialect dialect = new OceanBaseMySQLDialect(null);

    @Override
    public Record serialize(OceanBaseTestData data) {
        TableId tableId =
                new TableId(dialect::getFullTableName, data.getSchemaName(), data.getTableName());
        if (data.getSql() != null) {
            return new SchemaChangeRecord(tableId, data.getSql(), data.getSqlType());
        }
        TableInfo tableInfo = new TableInfo(tableId, data.getResolvedSchema());
        OceanBaseRowDataSerializationSchema serializationSchema =
                new OceanBaseRowDataSerializationSchema(tableInfo);
        return serializationSchema.serialize(data.getRowData());
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
