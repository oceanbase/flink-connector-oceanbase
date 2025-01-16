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
package com.oceanbase.connector.flink.utils;

import com.oceanbase.connector.flink.tools.catalog.OceanBaseSchemaFactory;
import com.oceanbase.connector.flink.tools.catalog.TableSchema;

import org.apache.flink.util.function.SupplierWithException;

import org.apache.commons.compress.utils.Lists;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

import static org.apache.flink.util.Preconditions.checkArgument;

public class OceanBaseToolsJdbcUtils extends OceanBaseJdbcUtils {
    private static final List<String> builtinDatabases =
            Collections.singletonList("information_schema");

    public static List<String> listDatabases(
            SupplierWithException<Connection, SQLException> connectionSupplier) {
        return extractColumnValuesBySQL(
                connectionSupplier,
                "SELECT `SCHEMA_NAME` FROM `INFORMATION_SCHEMA`.`SCHEMATA`;",
                1,
                dbName -> !builtinDatabases.contains(dbName));
    }

    public static boolean databaseExists(
            String database, SupplierWithException<Connection, SQLException> connectionSupplier) {
        checkArgument(!org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly(database));
        return listDatabases(connectionSupplier).contains(database);
    }

    public static void createDatabase(
            String database, SupplierWithException<Connection, SQLException> connectionSupplier) {
        execute(connectionSupplier, String.format("CREATE DATABASE IF NOT EXISTS %s", database));
    }

    public static boolean tableExists(
            String database,
            String table,
            SupplierWithException<Connection, SQLException> connectionSupplier) {
        return databaseExists(database, connectionSupplier)
                && listTables(database, connectionSupplier).contains(table);
    }

    public static List<String> listTables(
            String databaseName,
            SupplierWithException<Connection, SQLException> connectionSupplier) {
        if (!databaseExists(databaseName, connectionSupplier)) {
            throw new RuntimeException("database" + databaseName + " is not exists");
        }
        return extractColumnValuesBySQL(
                connectionSupplier,
                "SELECT TABLE_NAME FROM information_schema.`TABLES` WHERE TABLE_SCHEMA = ?",
                1,
                null,
                databaseName);
    }

    public static void createTable(
            TableSchema schema,
            SupplierWithException<Connection, SQLException> connectionSupplier) {
        String ddl = buildCreateTableDDL(schema);
        execute(connectionSupplier, ddl);
    }

    public static void execute(
            SupplierWithException<Connection, SQLException> connectionSupplier, String sql) {
        try (Connection connection = connectionSupplier.get();
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("SQL query could not be executed: %s", sql), e);
        }
    }

    public static List<String> extractColumnValuesBySQL(
            SupplierWithException<Connection, SQLException> connectionSupplier,
            String sql,
            int columnIndex,
            Predicate<String> filterFunc,
            Object... params) {
        List<String> columnValues = Lists.newArrayList();
        try (Connection connection = connectionSupplier.get();
                PreparedStatement ps = connection.prepareStatement(sql)) {
            if (Objects.nonNull(params) && params.length > 0) {
                for (int i = 0; i < params.length; i++) {
                    ps.setObject(i + 1, params[i]);
                }
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String columnValue = rs.getString(columnIndex);
                    if (filterFunc == null || filterFunc.test(columnValue)) {
                        columnValues.add(columnValue);
                    }
                }
            }
            return columnValues;
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("The following SQL query could not be executed: %s", sql), e);
        }
    }

    public static String buildCreateTableDDL(TableSchema schema) {
        return OceanBaseSchemaFactory.generateCreateTableDDL(schema);
    }
}
