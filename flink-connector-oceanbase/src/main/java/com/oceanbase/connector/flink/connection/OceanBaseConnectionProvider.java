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

import com.oceanbase.connector.flink.dialect.OceanBaseDialect;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public interface OceanBaseConnectionProvider extends AutoCloseable, Serializable {

    /**
     * Attempts to establish a connection
     *
     * @return a connection to OceanBase
     * @throws SQLException if a database access error occurs
     */
    Connection getConnection() throws SQLException;

    /**
     * Get connection info
     *
     * @return connection info
     */
    OceanBaseConnectionInfo getConnectionInfo();

    /**
     * Get table partition info
     *
     * @return table partition info
     */
    OceanBaseTablePartInfo getTablePartInfo();

    /**
     * Attempts to get the version of OceanBase
     *
     * @return version
     * @throws SQLException if a database access error occurs
     */
    default String getVersion(OceanBaseDialect dialect) throws SQLException {
        try (Connection conn = getConnection();
                Statement statement = conn.createStatement()) {
            try {
                ResultSet rs = statement.executeQuery(dialect.getSelectOBVersionStatement());
                if (rs.next()) {
                    return rs.getString(1);
                }
            } catch (SQLException e) {
                if (!e.getMessage().contains("not exist")) {
                    throw e;
                }
            }

            ResultSet rs = statement.executeQuery(dialect.getQueryVersionCommentStatement());
            if (rs.next()) {
                String versionComment = rs.getString("VALUE");
                String[] parts = StringUtils.split(versionComment, " ");
                if (parts != null && parts.length > 1) {
                    return parts[1];
                }
            }
            return null;
        }
    }
}
