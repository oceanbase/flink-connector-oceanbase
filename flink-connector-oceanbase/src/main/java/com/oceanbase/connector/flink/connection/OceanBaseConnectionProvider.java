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

import com.oceanbase.connector.flink.OceanBaseConnectorOptions;
import com.oceanbase.connector.flink.dialect.OceanBaseDialect;
import com.oceanbase.connector.flink.dialect.OceanBaseMySQLDialect;
import com.oceanbase.connector.flink.dialect.OceanBaseOracleDialect;

import com.alibaba.druid.pool.DruidDataSource;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class OceanBaseConnectionProvider implements ConnectionProvider {

    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseConnectionProvider.class);

    private static final long serialVersionUID = 1L;

    enum CompatibleMode {
        MYSQL,
        ORACLE;

        public static CompatibleMode parse(@Nonnull String text) {
            switch (text.trim().toUpperCase()) {
                case "MYSQL":
                    return MYSQL;
                case "ORACLE":
                    return ORACLE;
                default:
                    throw new UnsupportedOperationException("Unsupported compatible mode: " + text);
            }
        }

        public boolean isMySqlMode() {
            return this == MYSQL;
        }
    }

    public static class Version {

        private final String text;

        Version(String text) {
            this.text = text;
        }

        public String getText() {
            return text;
        }

        public boolean isV4() {
            return StringUtils.isNoneBlank(text) && text.startsWith("4.");
        }

        @Override
        public String toString() {
            return "Version{" + "text='" + text + '\'' + '}';
        }
    }

    private final OceanBaseConnectorOptions options;
    private final CompatibleMode compatibleMode;
    private final OceanBaseDialect dialect;

    private Version version;

    private transient volatile boolean inited = false;
    private transient DataSource dataSource;

    public OceanBaseConnectionProvider(OceanBaseConnectorOptions options) {
        this.options = options;
        this.compatibleMode = CompatibleMode.parse(options.getCompatibleMode());
        this.dialect =
                compatibleMode.isMySqlMode()
                        ? new OceanBaseMySQLDialect()
                        : new OceanBaseOracleDialect();
    }

    public boolean isMySqlMode() {
        return compatibleMode.isMySqlMode();
    }

    public OceanBaseDialect getDialect() {
        return dialect;
    }

    public void init() {
        if (!inited) {
            synchronized (this) {
                if (!inited) {
                    DruidDataSource druidDataSource = new DruidDataSource();
                    druidDataSource.setUrl(options.getUrl());
                    druidDataSource.setUsername(options.getUsername());
                    druidDataSource.setPassword(options.getPassword());
                    druidDataSource.setDriverClassName(options.getDriverClassName());
                    Properties properties = options.getDruidProperties();
                    if (properties != null) {
                        druidDataSource.configFromPropeties(properties);
                    }
                    dataSource = druidDataSource;
                    inited = true;
                }
            }
        }
    }

    public Connection getConnection() throws SQLException {
        init();
        return dataSource.getConnection();
    }

    public Version getVersion() {
        if (version == null) {
            try {
                String versionText = queryVersion();
                LOG.info("Got OceanBase version number: {}", versionText);
                version = new Version(versionText);
            } catch (SQLException e) {
                throw new RuntimeException("Failed to query version of OceanBase", e);
            }
        }
        return version;
    }

    private String queryVersion() throws SQLException {
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
                throw new RuntimeException("Illegal 'version_comment': " + versionComment);
            }
            throw new RuntimeException("'version_comment' not found");
        }
    }

    @Override
    public void close() throws Exception {
        if (dataSource != null) {
            if (dataSource instanceof AutoCloseable) {
                ((AutoCloseable) dataSource).close();
            }
            dataSource = null;
        }
        inited = false;
    }
}
