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

package com.oceanbase.connector.flink.cdc;

import com.oceanbase.connector.flink.table.FieldSchema;
import com.oceanbase.connector.flink.table.TableId;
import com.oceanbase.connector.flink.table.TableInfo;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.StringUtils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

public abstract class SourceSchema {
    protected final String databaseName;
    protected final String schemaName;
    protected final String tableName;
    protected final String tableComment;
    protected LinkedHashMap<String, FieldSchema> fields;
    public List<String> primaryKeys;

    public SourceSchema(
            String databaseName, String schemaName, String tableName, String tableComment)
            throws Exception {
        this.databaseName = databaseName;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.tableComment = tableComment;
    }

    public abstract String convertToOceanBaseType(
            String fieldType, Integer precision, Integer scale);

    public String getTableIdentifier() {
        return getString(databaseName, schemaName, tableName);
    }

    public static String getString(String databaseName, String schemaName, String tableName) {
        StringJoiner identifier = new StringJoiner(".");
        if (!StringUtils.isNullOrWhitespaceOnly(databaseName)) {
            identifier.add(databaseName);
        }
        if (!StringUtils.isNullOrWhitespaceOnly(schemaName)) {
            identifier.add(schemaName);
        }

        if (!StringUtils.isNullOrWhitespaceOnly(tableName)) {
            identifier.add(tableName);
        }

        return identifier.toString();
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public Map<String, FieldSchema> getFields() {
        return fields;
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public String getTableComment() {
        return tableComment;
    }

    public TableInfo toTableInfo() {
        List<String> fieldNames = new ArrayList<>();
        List<LogicalType> fieldTypes = new ArrayList<>();
        for (FieldSchema fieldSchema : getFields().values()) {
            fieldNames.add(fieldSchema.getName());
            fieldTypes.add(fieldSchema.getType());
        }
        return new TableInfo(
                new TableId(getDatabaseName(), getTableName()),
                getPrimaryKeys(),
                fieldNames,
                fieldTypes,
                null);
    }
}
