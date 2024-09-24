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

package com.oceanbase.connector.flink.connection;

import com.oceanbase.connector.flink.OceanBaseConnectorOptions;
import com.oceanbase.connector.flink.dialect.OceanBaseDialect;
import com.oceanbase.connector.flink.dialect.OceanBaseMySQLDialect;
import com.oceanbase.connector.flink.dialect.OceanBaseOracleDialect;
import com.oceanbase.connector.flink.directload.DirectLoader;
import com.oceanbase.connector.flink.directload.DirectLoaderBuilder;
import com.oceanbase.connector.flink.table.TableId;
import com.oceanbase.connector.flink.utils.OceanBaseJdbcUtils;

import com.alibaba.druid.pool.DruidDataSource;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class OceanBaseConnectionProvider implements ConnectionProvider {

    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseConnectionProvider.class);

    private static final long serialVersionUID = 1L;

    private final OceanBaseConnectorOptions options;
    private final OceanBaseDialect dialect;

    private OceanBaseVersion version;
    private OceanBaseUserInfo userInfo;

    private transient volatile boolean inited = false;
    private transient DataSource dataSource;

    public OceanBaseConnectionProvider(OceanBaseConnectorOptions options) {
        this.options = options;
        this.dialect =
                "MySQL".equalsIgnoreCase(getCompatibleMode().trim())
                        ? new OceanBaseMySQLDialect()
                        : new OceanBaseOracleDialect(options);
    }

    private String getCompatibleMode() {
        try {
            Class.forName(options.getDriverClassName());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(
                    "Failed to load driver class: " + options.getDriverClassName(), e);
        }
        String mode =
                OceanBaseJdbcUtils.getCompatibleMode(
                        () ->
                                DriverManager.getConnection(
                                        options.getUrl(),
                                        options.getUsername(),
                                        options.getPassword()));
        if (StringUtils.isBlank(mode)) {
            throw new RuntimeException("Query found empty compatible mode");
        }
        LOG.info("Query found OceanBase compatible mode: {}", mode);
        return mode;
    }

    public OceanBaseDialect getDialect() {
        return dialect;
    }

    protected void init() {
        if (!inited) {
            synchronized (this) {
                if (!inited) {
                    DruidDataSource druidDataSource = new DruidDataSource();
                    druidDataSource.setUrl(options.getUrl());
                    druidDataSource.setUsername(options.getUsername());
                    druidDataSource.setPassword(options.getPassword());
                    druidDataSource.setDriverClassName(options.getDriverClassName());
                    druidDataSource.setConnectProperties(
                            initializeDefaultJdbcProperties(options.getUrl()));
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

    private Properties initializeDefaultJdbcProperties(String jdbcUrl) {
        Properties defaultJdbcProperties = new Properties();
        defaultJdbcProperties.setProperty("useSSL", "false");
        defaultJdbcProperties.setProperty("rewriteBatchedStatements", "true");
        defaultJdbcProperties.setProperty("initialTimeout", "2");
        defaultJdbcProperties.setProperty("autoReconnect", "true");
        defaultJdbcProperties.setProperty("maxReconnects", "3");

        defaultJdbcProperties.setProperty("useInformationSchema", "true");
        defaultJdbcProperties.setProperty("nullCatalogMeansCurrent", "false");
        defaultJdbcProperties.setProperty("useUnicode", "true");
        defaultJdbcProperties.setProperty("zeroDateTimeBehavior", "convertToNull");
        defaultJdbcProperties.setProperty("characterEncoding", "UTF-8");
        defaultJdbcProperties.setProperty("characterSetResults", "UTF-8");

        // Avoid overwriting user's custom jdbc properties.
        List<String> jdbcUrlProperties =
                defaultJdbcProperties.keySet().stream()
                        .map(Object::toString)
                        .filter(jdbcUrl::contains)
                        .collect(Collectors.toList());
        jdbcUrlProperties.forEach(defaultJdbcProperties::remove);

        return defaultJdbcProperties;
    }

    public Connection getConnection() throws SQLException {
        init();
        return dataSource.getConnection();
    }

    public OceanBaseVersion getVersion() {
        if (version == null) {
            String versionComment = OceanBaseJdbcUtils.getVersionComment(this::getConnection);
            LOG.info("Query found version comment: {}", versionComment);
            version = OceanBaseVersion.fromVersionComment(versionComment);
        }
        return version;
    }

    public OceanBaseUserInfo getUserInfo() {
        if (userInfo == null) {
            OceanBaseUserInfo user = OceanBaseUserInfo.parse(options.getUsername());
            if (user.getCluster() == null) {
                String cluster = OceanBaseJdbcUtils.getClusterName(this::getConnection);
                if (StringUtils.isBlank(cluster)) {
                    throw new RuntimeException("Query found empty cluster name");
                }
                user.setCluster(cluster);
            }
            if (user.getTenant() == null) {
                String tenant = OceanBaseJdbcUtils.getTenantName(this::getConnection, dialect);
                if (StringUtils.isBlank(tenant)) {
                    throw new RuntimeException("Query found empty tenant name");
                }
                user.setTenant(tenant);
            }
            userInfo = user;
        }
        return userInfo;
    }

    public DirectLoader getDirectLoadStatement(TableId tableId) {
        int count = OceanBaseJdbcUtils.getTableRowsCount(this::getConnection, tableId.identifier());
        if (count != 0) {
            throw new RuntimeException(
                    "Direct load can only work on empty table, while table "
                            + tableId.identifier()
                            + " has "
                            + count
                            + " rows");
        }
        DirectLoader directLoader =
                new DirectLoaderBuilder()
                        .host(options.getDirectLoadHost())
                        .port(options.getDirectLoadPort())
                        .user(getUserInfo().getUser())
                        .password(options.getPassword())
                        .tenant(getUserInfo().getTenant())
                        .schema(options.getSchemaName())
                        .table(tableId.getTableName())
                        .duplicateKeyAction(options.getDirectLoadDupAction())
                        .maxErrorCount(options.getDirectLoadMaxErrorRows())
                        .directLoadMethod(options.getDirectLoadLoadMethod())
                        .timeout(options.getDirectLoadTimeout())
                        .heartBeatTimeout(options.getDirectLoadHeartbeatTimeout())
                        .heartBeatInterval(options.getDirectLoadHeartbeatInterval())
                        .parallel(options.getDirectLoadParallel())
                        .build();

        return directLoader;
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
