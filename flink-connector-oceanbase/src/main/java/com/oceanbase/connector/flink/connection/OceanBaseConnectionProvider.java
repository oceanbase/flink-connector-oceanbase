/*
 * Copyright (c) 2023 OceanBase.
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
