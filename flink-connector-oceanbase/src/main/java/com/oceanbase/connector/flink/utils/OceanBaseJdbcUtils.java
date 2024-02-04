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

import com.oceanbase.connector.flink.dialect.OceanBaseDialect;

import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.function.SupplierWithException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class OceanBaseJdbcUtils {

    public static int getTableRowsCount(
            SupplierWithException<Connection, SQLException> connectionSupplier, String tableName) {
        return (int)
                query(
                        connectionSupplier,
                        "SELECT COUNT(1) FROM " + tableName,
                        rs -> rs.next() ? rs.getInt(1) : 0);
    }

    public static String getVersionComment(
            SupplierWithException<Connection, SQLException> connectionSupplier) {
        return (String)
                query(
                        connectionSupplier,
                        "SHOW VARIABLES LIKE 'version_comment'",
                        rs -> rs.next() ? rs.getString("VALUE") : null);
    }

    public static String getCompatibleMode(
            SupplierWithException<Connection, SQLException> connectionSupplier) {
        return (String)
                query(
                        connectionSupplier,
                        "SHOW VARIABLES LIKE 'ob_compatibility_mode'",
                        rs -> rs.next() ? rs.getString("VALUE") : null);
    }

    public static String getClusterName(
            SupplierWithException<Connection, SQLException> connectionSupplier) {
        return (String)
                query(
                        connectionSupplier,
                        "SHOW PARAMETERS LIKE 'cluster'",
                        rs -> rs.next() ? rs.getString("VALUE") : null);
    }

    public static String getTenantName(
            SupplierWithException<Connection, SQLException> connectionSupplier,
            OceanBaseDialect dialect) {
        return (String)
                query(
                        connectionSupplier,
                        dialect.getQueryTenantNameStatement(),
                        rs -> rs.next() ? rs.getString(1) : null);
    }

    private static Object query(
            SupplierWithException<Connection, SQLException> connectionSupplier,
            String sql,
            FunctionWithException<ResultSet, Object, SQLException> resultSetConsumer) {
        try (Connection connection = connectionSupplier.get();
                Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery(sql);
            return resultSetConsumer.apply(rs);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute sql: " + sql, e);
        }
    }
}
