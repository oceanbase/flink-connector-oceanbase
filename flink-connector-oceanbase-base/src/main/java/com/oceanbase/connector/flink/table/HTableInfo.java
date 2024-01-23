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

import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class HTableInfo implements Table {

    private final String databaseName;
    private final String tableName;

    private final String rowKeyName;
    private final LogicalType rowKeyType;
    private final Map<String, Integer> fieldIndexMap;
    private final List<String> familyNames;
    private final Map<String, String[]> columnNameMap;
    private final Map<String, LogicalType[]> columnTypeMap;

    public HTableInfo(String databaseName, String tableName, ResolvedSchema resolvedSchema) {
        this(
                databaseName,
                tableName,
                rowKeyColumn(resolvedSchema).getName(),
                rowKeyColumn(resolvedSchema).getDataType().getLogicalType(),
                columns(resolvedSchema).stream().map(Column::getName).collect(Collectors.toList()),
                familyColumns(resolvedSchema).stream()
                        .collect(
                                Collectors.toMap(
                                        Column::getName,
                                        family ->
                                                ((RowType) family.getDataType().getLogicalType())
                                                        .getFields().stream()
                                                                .map(RowType.RowField::getName)
                                                                .collect(Collectors.toList())
                                                                .toArray(new String[0]))),
                familyColumns(resolvedSchema).stream()
                        .collect(
                                Collectors.toMap(
                                        Column::getName,
                                        family ->
                                                ((RowType) family.getDataType().getLogicalType())
                                                        .getFields().stream()
                                                                .map(RowType.RowField::getType)
                                                                .collect(Collectors.toList())
                                                                .toArray(new LogicalType[0]))));
    }

    private static List<Column> columns(ResolvedSchema resolvedSchema) {
        return resolvedSchema.getColumns();
    }

    private static Column rowKeyColumn(ResolvedSchema resolvedSchema) {
        List<Column> columns =
                columns(resolvedSchema).stream()
                        .filter(
                                column ->
                                        column.getDataType().getLogicalType().getTypeRoot()
                                                != LogicalTypeRoot.ROW)
                        .collect(Collectors.toList());
        if (columns.size() != 1) {
            throw new IllegalArgumentException(
                    "There should be exactly one field that is not ROW type.");
        }
        return columns.get(0);
    }

    private static List<Column> familyColumns(ResolvedSchema resolvedSchema) {
        return columns(resolvedSchema).stream()
                .filter(
                        column ->
                                column.getDataType().getLogicalType().getTypeRoot()
                                        == LogicalTypeRoot.ROW)
                .collect(Collectors.toList());
    }

    public HTableInfo(
            String databaseName,
            String tableName,
            String rowKeyName,
            LogicalType rowKeyType,
            List<String> fieldNames,
            Map<String, String[]> columnNameMap,
            Map<String, LogicalType[]> columnTypeMap) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.rowKeyName = rowKeyName;
        this.rowKeyType = rowKeyType;
        this.familyNames =
                fieldNames.stream().filter(s -> !rowKeyName.equals(s)).collect(Collectors.toList());
        this.columnNameMap = columnNameMap;
        this.columnTypeMap = columnTypeMap;
        this.fieldIndexMap =
                IntStream.range(0, fieldNames.size())
                        .boxed()
                        .collect(Collectors.toMap(fieldNames::get, i -> i));
    }

    @Override
    public String getTableId() {
        return databaseName + "." + tableName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getRowKeyName() {
        return rowKeyName;
    }

    public LogicalType getRowKeyType() {
        return rowKeyType;
    }

    public List<String> getFamilyNames() {
        return familyNames;
    }

    public String[] getColumnNames(String familyName) {
        return columnNameMap.get(familyName);
    }

    public LogicalType[] getColumnTypes(String familyName) {
        return columnTypeMap.get(familyName);
    }

    @Override
    public Integer getFieldIndex(String fieldName) {
        return fieldIndexMap.get(fieldName);
    }
}
