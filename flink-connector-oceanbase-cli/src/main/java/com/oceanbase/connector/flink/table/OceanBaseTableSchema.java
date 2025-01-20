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

import com.oceanbase.connector.flink.dialect.OceanBaseDialect;
import com.oceanbase.connector.flink.dialect.OceanBaseMySQLDialect;
import com.oceanbase.connector.flink.source.FieldSchema;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** The schema information of OceanBase table. */
public class OceanBaseTableSchema {

    private final OceanBaseDialect dialect;
    private final TableId tableId;
    private final String comment;
    private final List<FieldSchema> fields;
    private final List<String> keys;
    private final Map<String, String> properties;

    public OceanBaseTableSchema(
            TableId tableId, String comment, List<FieldSchema> fields, List<String> keys) {
        this(new OceanBaseMySQLDialect(), tableId, comment, fields, keys, Collections.emptyMap());
    }

    public OceanBaseTableSchema(
            OceanBaseDialect dialect,
            TableId tableId,
            String comment,
            List<FieldSchema> fields,
            List<String> keys,
            Map<String, String> properties) {
        this.dialect = dialect;
        this.tableId = tableId;
        this.comment = comment;
        this.fields = fields;
        this.keys = keys;
        this.properties = properties;
    }

    public String generateCreateTableDDL() {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ");
        sb.append(dialect.quoteIdentifier(tableId.getSchemaName()))
                .append(".")
                .append(dialect.quoteIdentifier(tableId.getTableName()))
                .append(" (");

        for (FieldSchema fieldSchema : fields) {
            sb.append(dialect.quoteIdentifier(fieldSchema.getName()))
                    .append(" ")
                    .append(fieldSchema.getType().getObType());

            if (!keys.contains(fieldSchema.getName()) && fieldSchema.getNullable()) {
                sb.append(" NULL");
            } else {
                sb.append(" NOT NULL");
            }

            if (fieldSchema.getDefaultValue() != null) {
                sb.append(" DEFAULT ").append(quoteDefaultValue(fieldSchema.getDefaultValue()));
            }

            if (fieldSchema.getComment() != null && !fieldSchema.getComment().trim().isEmpty()) {
                sb.append(" COMMENT '").append(quoteComment(fieldSchema.getComment())).append("'");
            }
            sb.append(", ");
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
        if (comment != null && !comment.trim().isEmpty()) {
            sb.append(" COMMENT='").append(quoteComment(comment)).append("'");
        }
        sb.append(";");

        return sb.toString();
    }

    public String quoteDefaultValue(String defaultValue) {
        // DEFAULT current_timestamp not need quote
        if (defaultValue.equalsIgnoreCase("current_timestamp")) {
            return defaultValue;
        }
        return "'" + defaultValue + "'";
    }

    public String quoteComment(String comment) {
        if (comment == null) {
            return "";
        } else {
            return comment.replaceAll("'", "\\\\'");
        }
    }

    @Override
    public String toString() {
        return "OceanBaseTableSchema{"
                + "tableId="
                + tableId
                + ", comment='"
                + comment
                + '\''
                + ", fields="
                + fields
                + ", keys="
                + keys
                + ", properties="
                + properties
                + '}';
    }
}
