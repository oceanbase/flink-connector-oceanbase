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

import com.oceanbase.connector.flink.connection.OceanBaseConnectionProvider;
import com.oceanbase.connector.flink.dialect.OceanBaseOracleDialect;
import com.oceanbase.connector.flink.table.OceanBaseTableSchema;

import org.apache.commons.compress.utils.Lists;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class OceanBaseCatalogUtils extends OceanBaseJdbcUtils {

    private static final Set<String> builtinDatabases =
            new HashSet<String>() {
                {
                    add("__public");
                    add("information_schema");
                    add("mysql");
                    add("oceanbase");
                    add("LBACSYS");
                    add("ORAAUDITOR");
                }
            };

    @SuppressWarnings("unchecked")
    public static List<String> listDatabases(OceanBaseConnectionProvider connectionProvider) {
        List<String> databases = Lists.newArrayList();
        return (List<String>)
                query(
                        connectionProvider::getConnection,
                        connectionProvider.getDialect().getListSchemaStatement(),
                        rs -> {
                            while (rs.next()) {
                                String database = rs.getString(1);
                                if (!builtinDatabases.contains(database)) {
                                    databases.add(database);
                                }
                            }
                            return databases;
                        });
    }

    public static boolean databaseExists(
            OceanBaseConnectionProvider connectionProvider, String database) {
        return listDatabases(connectionProvider).contains(database);
    }

    public static void createDatabase(
            OceanBaseConnectionProvider connectionProvider, String database) {
        if (connectionProvider.getDialect() instanceof OceanBaseOracleDialect) {
            throw new UnsupportedOperationException();
        }
        execute(
                connectionProvider::getConnection,
                String.format("CREATE DATABASE IF NOT EXISTS %s", database));
    }

    @SuppressWarnings("unchecked")
    public static List<String> listTables(
            OceanBaseConnectionProvider connectionProvider, String databaseName) {
        if (!databaseExists(connectionProvider, databaseName)) {
            throw new RuntimeException("database" + databaseName + " is not exists");
        }
        List<String> tables = Lists.newArrayList();
        return (List<String>)
                query(
                        connectionProvider::getConnection,
                        connectionProvider.getDialect().getListTableStatement(databaseName),
                        rs -> {
                            while (rs.next()) {
                                tables.add(rs.getString(1));
                            }
                            return tables;
                        });
    }

    public static boolean tableExists(
            OceanBaseConnectionProvider connectionProvider, String database, String table) {
        return databaseExists(connectionProvider, database)
                && listTables(connectionProvider, database).contains(table);
    }

    public static void createTable(
            OceanBaseConnectionProvider connectionProvider, OceanBaseTableSchema schema) {
        execute(connectionProvider::getConnection, schema.generateCreateTableDDL());
    }
}
