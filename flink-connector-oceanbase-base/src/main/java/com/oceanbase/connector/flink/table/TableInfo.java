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

package com.oceanbase.connector.flink.table;

import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TableInfo implements Table {

    private static final long serialVersionUID = 1L;

    private final String schemaName;
    private final String tableName;
    private final List<String> primaryKey;
    private final List<String> fieldNames;
    private final Map<String, Integer> fieldIndexMap;
    private final List<LogicalType> dataTypes;

    public TableInfo(String schemaName, String tableName, ResolvedSchema resolvedSchema) {
        this(
                schemaName,
                tableName,
                resolvedSchema
                        .getPrimaryKey()
                        .map(UniqueConstraint::getColumns)
                        .orElseThrow(
                                () ->
                                        new UnsupportedOperationException(
                                                "Table without PK is not supported.")),
                resolvedSchema.getColumnNames(),
                resolvedSchema.getColumnDataTypes().stream()
                        .map(DataType::getLogicalType)
                        .collect(Collectors.toList()));
    }

    public TableInfo(
            String schemaName,
            String tableName,
            List<String> primaryKey,
            List<String> fieldNames,
            List<LogicalType> dataTypes) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.primaryKey = primaryKey;
        this.fieldNames = fieldNames;
        this.dataTypes = dataTypes;
        this.fieldIndexMap =
                IntStream.range(0, fieldNames.size())
                        .boxed()
                        .collect(Collectors.toMap(fieldNames::get, i -> i));
    }

    @Override
    public String getTableId() {
        return schemaName + "." + tableName;
    }

    @Override
    public Integer getFieldIndex(String fieldName) {
        return fieldIndexMap.get(fieldName);
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getPrimaryKey() {
        return primaryKey;
    }

    public List<String> getFieldNames() {
        return fieldNames;
    }

    public List<LogicalType> getDataTypes() {
        return dataTypes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableInfo that = (TableInfo) o;
        return Objects.equals(this.schemaName, that.schemaName)
                && Objects.equals(this.tableName, that.tableName)
                && Objects.equals(this.primaryKey, that.primaryKey)
                && Objects.equals(this.fieldNames, that.fieldNames)
                && Objects.equals(this.fieldIndexMap, that.fieldIndexMap)
                && Objects.equals(this.dataTypes, that.dataTypes);
    }
}
