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

import javax.annotation.Nonnull;

import java.io.Serializable;

public class TableId implements Serializable {
    private static final long serialVersionUID = 1L;

    @FunctionalInterface
    public interface Identifier extends Serializable {
        String identifier(String schemaName, String tableName);
    }

    private final Identifier identifier;
    private final String schemaName;
    private final String tableName;

    public TableId(@Nonnull String schemaName, @Nonnull String tableName) {
        this(
                (schema, table) -> String.format("\"%s\".\"%s\"", schema, table),
                schemaName,
                tableName);
    }

    public TableId(
            @Nonnull Identifier identifier, @Nonnull String schemaName, @Nonnull String tableName) {
        this.identifier = identifier;
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    public String identifier() {
        return identifier.identifier(schemaName, tableName);
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }
}
