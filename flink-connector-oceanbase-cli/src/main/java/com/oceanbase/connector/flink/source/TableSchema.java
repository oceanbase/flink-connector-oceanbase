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

package com.oceanbase.connector.flink.source;

import com.oceanbase.connector.flink.table.TableId;
import com.oceanbase.connector.flink.table.TableInfo;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

/** Schema information of generic data sources. */
public class TableSchema {

    public final String databaseName;
    public final String schemaName;
    public final String tableName;
    public final String tableComment;
    public final List<FieldSchema> fields = new ArrayList<>();
    public final List<String> primaryKeys = new ArrayList<>();

    public TableSchema(
            String databaseName, String schemaName, String tableName, String tableComment) {
        this.databaseName = databaseName;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.tableComment = tableComment;
    }

    public String getTableIdentifier() {
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

    public List<FieldSchema> getFields() {
        return fields;
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public String getTableComment() {
        return tableComment;
    }

    public void addField(FieldSchema fieldSchema) {
        this.fields.add(fieldSchema);
    }

    public void addPrimaryKey(String primaryKey) {
        this.primaryKeys.add(primaryKey);
    }

    public TableInfo toTableInfo() {
        List<String> fieldNames = new ArrayList<>();
        List<LogicalType> fieldTypes = new ArrayList<>();
        for (FieldSchema fieldSchema : getFields()) {
            fieldNames.add(fieldSchema.getName());
            fieldTypes.add(fieldSchema.getType().getFlinkType());
        }
        return new TableInfo(
                new TableId(getDatabaseName(), getTableName()),
                getPrimaryKeys(),
                fieldNames,
                fieldTypes,
                null);
    }
}
