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

public class SchemaChangeRecord implements Record {

    private static final long serialVersionUID = 1L;

    public enum Type {
        ALTER,
        CREATE,
        DROP,
        TRUNCATE
    }

    private final String tableId;
    private final String sql;
    private final Type type;

    public SchemaChangeRecord(String tableId, String sql, Type type) {
        this.tableId = tableId;
        this.sql = sql;
        this.type = type;
    }

    @Override
    public String getTableId() {
        return tableId;
    }

    public Type getType() {
        return type;
    }

    public boolean shouldRefreshSchema() {
        return getType() != Type.TRUNCATE;
    }

    public String getSql() {
        return sql;
    }

    @Override
    public String toString() {
        return "SchemaChangeRecord{"
                + "tableId='"
                + tableId
                + '\''
                + ", sql='"
                + sql
                + '\''
                + ", type="
                + type
                + '}';
    }
}
