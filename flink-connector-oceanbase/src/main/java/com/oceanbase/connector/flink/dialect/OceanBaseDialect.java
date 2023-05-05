/*
 * Copyright (c) 2023 OceanBase
 * flink-connector-oceanbase is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *         http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package com.oceanbase.connector.flink.dialect;

import javax.annotation.Nonnull;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public interface OceanBaseDialect {

    /**
     * Quotes the identifier
     *
     * @param identifier identifier
     * @return the quoted identifier
     */
    String quoteIdentifier(@Nonnull String identifier);

    /**
     * Gets the upsert statement
     *
     * @param tableName table name
     * @param fieldNames field names list
     * @param uniqueKeyFields unique key field names list
     * @return the statement string
     */
    String getUpsertStatement(
            @Nonnull String tableName,
            @Nonnull List<String> fieldNames,
            @Nonnull List<String> uniqueKeyFields);

    /**
     * Gets the exist statement
     *
     * @param tableName table name
     * @param uniqueKeyFields unique key field names list
     * @return the statement string
     */
    default String getExistStatement(
            @Nonnull String tableName, @Nonnull List<String> uniqueKeyFields) {
        String conditionClause =
                uniqueKeyFields.stream()
                        .map(f -> String.format("%s = ?", quoteIdentifier(f)))
                        .collect(Collectors.joining(" AND "));
        return "SELECT 1 FROM " + quoteIdentifier(tableName) + " WHERE " + conditionClause;
    }

    /**
     * Gets the insert statement
     *
     * @param tableName table name
     * @param fieldNames field names list
     * @return the statement string
     */
    default String getInsertIntoStatement(
            @Nonnull String tableName, @Nonnull List<String> fieldNames) {
        String columns =
                fieldNames.stream().map(this::quoteIdentifier).collect(Collectors.joining(", "));
        String placeholders = String.join(", ", Collections.nCopies(fieldNames.size(), "?"));
        return "INSERT INTO "
                + quoteIdentifier(tableName)
                + "("
                + columns
                + ")"
                + " VALUES ("
                + placeholders
                + ")";
    }

    /**
     * Gets the update statement
     *
     * @param tableName table name
     * @param fieldNames field names list
     * @param uniqueKeyFields unique key field names list
     * @return the statement string
     */
    default String getUpdateStatement(
            @Nonnull String tableName,
            @Nonnull List<String> fieldNames,
            @Nonnull List<String> uniqueKeyFields) {
        String setClause =
                fieldNames.stream()
                        .filter(f -> !uniqueKeyFields.contains(f))
                        .map(f -> String.format("%s = ?", quoteIdentifier(f)))
                        .collect(Collectors.joining(", "));
        String conditionClause =
                uniqueKeyFields.stream()
                        .map(f -> String.format("%s = ?", quoteIdentifier(f)))
                        .collect(Collectors.joining(" AND "));
        return "UPDATE "
                + quoteIdentifier(tableName)
                + " SET "
                + setClause
                + " WHERE "
                + conditionClause;
    }

    /**
     * Gets the delete statement
     *
     * @param tableName table name
     * @param uniqueKeyFields unique key field names list
     * @return the statement string
     */
    default String getDeleteStatement(
            @Nonnull String tableName, @Nonnull List<String> uniqueKeyFields) {
        String conditionClause =
                uniqueKeyFields.stream()
                        .map(f -> String.format("%s = ?", quoteIdentifier(f)))
                        .collect(Collectors.joining(" AND "));
        return "DELETE FROM " + quoteIdentifier(tableName) + " WHERE " + conditionClause;
    }

    /**
     * Get the system database name
     *
     * @return the system database name
     */
    String getSysDatabase();

    default String getMemStoreExistStatement(double threshold) {
        return "SELECT 1 FROM "
                + getSysDatabase()
                + ".GV$OB_MEMSTORE WHERE MEMSTORE_USED > MEMSTORE_LIMIT * "
                + threshold;
    }

    default String getLegacyMemStoreExistStatement(double threshold) {
        return "SELECT 1 FROM "
                + getSysDatabase()
                + ".GV$MEMSTORE WHERE TOTAL > MEM_LIMIT * "
                + threshold;
    }

    /**
     * Get the select statement for OB_VERSION() function
     *
     * @return the select statement for OB_VERSION() function
     */
    String getSelectOBVersionStatement();
}
