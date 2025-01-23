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

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Date;
import java.sql.Time;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class OceanBaseJsonSerializationSchema extends AbstractRecordSerializationSchema<String> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG =
            LoggerFactory.getLogger(OceanBaseJsonSerializationSchema.class);

    private final TableInfo tableInfo;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public OceanBaseJsonSerializationSchema(TableInfo tableInfo) {
        this.tableInfo = tableInfo;
    }

    @Override
    public Record serialize(String rowDataStr) {
        try {
            JsonNode rowDataNode = objectMapper.readTree(rowDataStr);
            DataChangeRecord.Type type;
            String op = rowDataNode.path("op").asText();
            if ("r".equals(op) || "c".equals(op)) {
                type = DataChangeRecord.Type.UPSERT;
            } else if ("d".equals(op)) {
                type = DataChangeRecord.Type.DELETE;
            } else {
                throw new IllegalArgumentException("Unknown operation type: " + op);
            }
            int size = tableInfo.getFieldNames().size();
            Object[] values = new Object[size];
            for (int i = 0; i < size; i++) {
                String fieldName = tableInfo.getFieldNames().get(i);
                JsonNode fieldNode = rowDataNode.path("after").path(fieldName);
                values[i] = objectMapper.convertValue(fieldNode, new TypeReference<Object>() {});
            }

            return new DataChangeRecord(tableInfo, type, values);
        } catch (IOException e) {
            String errorMessage = "Failed to parse rowData JSON: " + rowDataStr;
            LOG.error(errorMessage, e);
            throw new RuntimeException(errorMessage);
        }
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
            case TIMESTAMP_WITH_TIME_ZONE:
                return data -> ((TimestampData) data).toInstant().toString();
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return data ->
                        ((TimestampData) data)
                                .toInstant()
                                .atZone(ZoneId.systemDefault())
                                .toString();
            case DECIMAL:
                return data -> ((DecimalData) data).toBigDecimal();
            case ARRAY:
                return data -> {
                    ArrayData arrayData = (ArrayData) data;
                    return IntStream.range(0, arrayData.size())
                            .mapToObj(i -> arrayData.getString(i).toString())
                            .collect(Collectors.joining(","));
                };
            case MAP:
                return data -> {
                    MapData mapData = (MapData) data;
                    ArrayData keyArray = mapData.keyArray();
                    ArrayData valueArray = mapData.valueArray();
                    return "{"
                            + IntStream.range(0, keyArray.size())
                                    .mapToObj(
                                            i ->
                                                    "\""
                                                            + keyArray.getString(i).toString()
                                                            + "\":\""
                                                            + valueArray.getString(i).toString()
                                                            + "\"")
                                    .collect(Collectors.joining(","))
                            + "}";
                };
            case ROW:
                return data -> {
                    RowData rowData = (RowData) data;
                    RowType rowType = (RowType) type;
                    StringBuilder sb = new StringBuilder();
                    sb.append("{");
                    for (int i = 0; i < rowData.getArity(); i++) {
                        if (i > 0) {
                            sb.append(",");
                        }
                        String fieldName = rowType.getFieldNames().get(i);
                        LogicalType fieldType = rowType.getFields().get(i).getType();
                        sb.append("\"").append(fieldName).append("\":");
                        if (fieldType instanceof VarCharType) {
                            sb.append("\"").append(rowData.getString(i).toString()).append("\"");
                        } else if (fieldType instanceof IntType) {
                            sb.append(rowData.getInt(i));
                        } else if (fieldType instanceof BooleanType) {
                            sb.append(rowData.getBoolean(i));
                        }
                        // Add more types as needed
                    }
                    sb.append("}");
                    return sb.toString();
                };
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }
}
