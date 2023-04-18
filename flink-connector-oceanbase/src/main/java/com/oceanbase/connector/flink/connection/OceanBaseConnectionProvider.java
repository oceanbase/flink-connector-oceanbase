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

package com.oceanbase.connector.flink.connection;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public interface OceanBaseConnectionProvider extends AutoCloseable {

    /**
     * Attempts to establish a connection
     *
     * @return a connection to OceanBase
     * @throws SQLException if a database access error occurs
     */
    Connection getConnection() throws SQLException;

    /**
     * Attempts to get the compatible mode of OceanBase
     *
     * @return compatible mode
     * @throws SQLException if a database access error occurs
     */
    default String getCompatibleMode() throws SQLException {
        try (Connection conn = getConnection();
                Statement statement = conn.createStatement()) {
            ResultSet rs = statement.executeQuery("SHOW VARIABLES LIKE 'ob_compatibility_mode'");
            if (rs.next()) {
                return rs.getString("Value");
            }
            return null;
        }
    }
}
