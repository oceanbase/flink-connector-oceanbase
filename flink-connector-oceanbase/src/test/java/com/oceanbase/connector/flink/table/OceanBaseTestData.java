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

import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;

import java.io.Serializable;

public class OceanBaseTestData implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String schemaName;
    private final String tableName;
    private final ResolvedSchema resolvedSchema;
    private final RowData rowData;
    private final SchemaChangeRecord.Type sqlType;
    private final String sql;

    public OceanBaseTestData(
            String schemaName, String tableName, ResolvedSchema resolvedSchema, RowData rowData) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.resolvedSchema = resolvedSchema;
        this.rowData = rowData;
        this.sqlType = null;
        this.sql = null;
    }

    public OceanBaseTestData(
            String schemaName, String tableName, SchemaChangeRecord.Type sqlType, String sql) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.resolvedSchema = null;
        this.rowData = null;
        this.sqlType = sqlType;
        this.sql = sql;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public ResolvedSchema getResolvedSchema() {
        return resolvedSchema;
    }

    public RowData getRowData() {
        return rowData;
    }

    public SchemaChangeRecord.Type getSqlType() {
        return sqlType;
    }

    public String getSql() {
        return sql;
    }
}
