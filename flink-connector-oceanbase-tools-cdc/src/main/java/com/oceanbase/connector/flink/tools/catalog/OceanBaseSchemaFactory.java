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
package com.oceanbase.connector.flink.tools.catalog; // Licensed to the Apache Software Foundation

import org.apache.flink.util.Preconditions;

import org.apache.commons.collections.CollectionUtils;

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
        tableSchema.setDistributeKeys(buildDistributeKeys(pkKeys, columnFields));
        return tableSchema;
    }

    private static List<String> buildDistributeKeys(
            List<String> primaryKeys, Map<String, FieldSchema> fields) {
        return buildKeys(primaryKeys, fields);
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
        sb.append(identifier(schema.getDatabase()))
                .append(".")
                .append(identifier(schema.getTable()))
                .append(" (");

        Map<String, FieldSchema> fields = schema.getFields();
        List<String> keys = schema.getKeys();

        // Append fields
        for (Map.Entry<String, FieldSchema> entry : fields.entrySet()) {
            FieldSchema field = entry.getValue();
            buildColumn(sb, field, keys.contains(entry.getKey()));
        }

        sb = sb.deleteCharAt(sb.length() - 1); // 删除最后一个逗号
        // Append primary key constraint
        if (!keys.isEmpty()) {
            sb.append("PRIMARY KEY (")
                    .append(
                            keys.stream()
                                    .map(OceanBaseSchemaFactory::identifier)
                                    .collect(Collectors.joining(",")))
                    .append(")");
        }
        sb.append(")");

        // Append table comment
        if (schema.getTableComment() != null && !schema.getTableComment().trim().isEmpty()) {
            sb.append(" COMMENT='").append(quoteComment(schema.getTableComment())).append("'");
        }
        sb.append(";");

        System.out.println("Generated DDL: " + sb);
        return sb.toString();
    }

    private static void buildColumn(StringBuilder sb, FieldSchema field, boolean isKey) {
        sb.append(identifier(field.getName())).append(" ").append(field.getTypeString());

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

    public static String identifier(String name) {
        if (name.startsWith("`") && name.endsWith("`")) {
            return name;
        }
        return "`" + name + "`";
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
        return identifier(dbTable[0]) + "." + identifier(dbTable[1]);
    }
}
