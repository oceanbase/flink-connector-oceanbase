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
package com.oceanbase.connector.flink.table; // Licensed to the Apache Software Foundation

import com.oceanbase.connector.flink.dialect.OceanBaseDialect;
import com.oceanbase.connector.flink.dialect.OceanBaseMySQLDialect;

import org.apache.flink.util.Preconditions;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

/**
 * Factory that creates oceanbase schema.
 *
 * <p>In the case where oceanbase schema needs to be created, it is best to create it through this
 * factory
 */
public class OceanBaseSchemaFactory {

    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseSchemaFactory.class);
    private static final OceanBaseDialect dialect = new OceanBaseMySQLDialect();

    public static TableSchema createTableSchema(
            String database,
            String table,
            Map<String, FieldSchema> columnFields,
            List<String> pkKeys,
            String tableComment) {
        TableSchema tableSchema = new TableSchema();
        tableSchema.setDatabase(database);
        tableSchema.setTable(table);
        tableSchema.setFields(columnFields);
        tableSchema.setKeys(buildKeys(pkKeys, columnFields));
        tableSchema.setTableComment(tableComment);
        return tableSchema;
    }

    /**
     * Theoretically, the duplicate table of oceanbase does not need to distinguish the key column,
     * but in the actual table creation statement, the key column will be automatically added. So if
     * it is a duplicate table, primaryKeys is empty, and we uniformly take the first field as the
     * key.
     */
    private static List<String> buildKeys(
            List<String> primaryKeys, Map<String, FieldSchema> fields) {
        if (CollectionUtils.isNotEmpty(primaryKeys)) {
            return primaryKeys;
        }
        if (!fields.isEmpty()) {
            Entry<String, FieldSchema> firstField = fields.entrySet().iterator().next();
            return Collections.singletonList(firstField.getKey());
        }
        return new ArrayList<>();
    }

    public static String generateCreateTableDDL(TableSchema schema) {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ");
        sb.append(dialect.quoteIdentifier(schema.getDatabase()))
                .append(".")
                .append(dialect.quoteIdentifier(schema.getTable()))
                .append(" (");

        Map<String, FieldSchema> fields = schema.getFields();
        List<String> keys = schema.getKeys();

        // Append fields
        for (Map.Entry<String, FieldSchema> entry : fields.entrySet()) {
            FieldSchema field = entry.getValue();
            buildColumn(sb, field, keys.contains(entry.getKey()));
        }

        sb = sb.deleteCharAt(sb.length() - 1);
        // Append primary key constraint
        if (!keys.isEmpty()) {
            sb.append("PRIMARY KEY (")
                    .append(
                            keys.stream()
                                    .map(dialect::quoteIdentifier)
                                    .collect(Collectors.joining(",")))
                    .append(")");
        }
        sb.append(")");

        // Append table comment
        if (schema.getTableComment() != null && !schema.getTableComment().trim().isEmpty()) {
            sb.append(" COMMENT='").append(quoteComment(schema.getTableComment())).append("'");
        }
        sb.append(";");

        LOG.info("Generated DDL: {}", sb);
        return sb.toString();
    }

    private static void buildColumn(StringBuilder sb, FieldSchema field, boolean isKey) {
        sb.append(dialect.quoteIdentifier(field.getName()))
                .append(" ")
                .append(field.getTypeString());

        if (!isKey && field.getNullable()) {
            sb.append(" NULL");
        } else {
            sb.append(" NOT NULL");
        }

        if (field.getDefaultValue() != null) {
            sb.append(" DEFAULT ").append(quoteDefaultValue(field.getDefaultValue()));
        }

        if (field.getComment() != null && !field.getComment().trim().isEmpty()) {
            sb.append(" COMMENT '").append(quoteComment(field.getComment())).append("'");
        }

        sb.append(", ");
    }

    public static String quoteDefaultValue(String defaultValue) {
        // DEFAULT current_timestamp not need quote
        if (defaultValue.equalsIgnoreCase("current_timestamp")) {
            return defaultValue;
        }
        return "'" + defaultValue + "'";
    }

    public static String quoteComment(String comment) {
        if (comment == null) {
            return "";
        } else {
            return comment.replaceAll("'", "\\\\'");
        }
    }

    public static String quoteTableIdentifier(String tableIdentifier) {
        String[] dbTable = tableIdentifier.split("\\.");
        Preconditions.checkArgument(dbTable.length == 2);
        return dialect.quoteIdentifier(dbTable[0]) + "." + dialect.quoteIdentifier(dbTable[1]);
    }
}
