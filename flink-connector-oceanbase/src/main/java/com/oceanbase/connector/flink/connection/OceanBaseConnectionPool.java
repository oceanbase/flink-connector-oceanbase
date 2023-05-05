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

import com.alibaba.druid.pool.DruidDataSource;
import com.oceanbase.connector.flink.dialect.OceanBaseDialect;
import com.oceanbase.connector.flink.dialect.OceanBaseMySQLDialect;
import com.oceanbase.connector.flink.dialect.OceanBaseOracleDialect;
import com.oceanbase.partition.calculator.helper.TableEntryExtractor;
import com.oceanbase.partition.calculator.helper.TableEntryExtractorV4;
import com.oceanbase.partition.calculator.model.TableEntry;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;

public class OceanBaseConnectionPool implements OceanBaseConnectionProvider, Serializable {

    private static final long serialVersionUID = 1L;

    private final OceanBaseConnectionOptions options;
    private DataSource dataSource;
    private volatile boolean inited = false;
    private OceanBaseConnectionInfo connectionInfo;
    private OceanBaseTablePartInfo tablePartitionInfo;

    public OceanBaseConnectionPool(OceanBaseConnectionOptions options) {
        this.options = options;
    }

    public void init() {
        if (!inited) {
            synchronized (this) {
                if (!inited) {
                    String dataSourcePool = options.getConnectionPool();
                    if (dataSourcePool == null) {
                        throw new UnsupportedOperationException("Option 'connection-pool' is null");
                    }
                    switch (dataSourcePool.trim().toLowerCase()) {
                        case "druid":
                            DruidDataSource druidDataSource = new DruidDataSource();
                            druidDataSource.setUrl(options.getUrl());
                            druidDataSource.setUsername(options.getUsername());
                            druidDataSource.setPassword(options.getPassword());
                            druidDataSource.setDriverClassName(options.getDriverClass());
                            druidDataSource.configFromPropety(
                                    options.getConnectionPoolProperties());
                            dataSource = druidDataSource;
                            break;
                        case "hikari":
                            HikariDataSource hikariDataSource = new HikariDataSource();
                            hikariDataSource.setJdbcUrl(options.getUrl());
                            hikariDataSource.setUsername(options.getUsername());
                            hikariDataSource.setPassword(options.getPassword());
                            hikariDataSource.setDriverClassName(options.getDriverClass());
                            hikariDataSource.setDataSourceProperties(
                                    options.getConnectionPoolProperties());
                            dataSource = hikariDataSource;
                            break;
                        default:
                            throw new UnsupportedOperationException(
                                    "Invalid connection pool: " + dataSourcePool);
                    }
                    inited = true;
                }
            }
        }
    }

    @Override
    public Connection getConnection() throws SQLException {
        init();
        return dataSource.getConnection();
    }

    @Override
    public OceanBaseConnectionInfo getConnectionInfo() {
        if (connectionInfo == null) {
            try {
                OceanBaseConnectionInfo.CompatibleMode compatibleMode =
                        OceanBaseConnectionInfo.CompatibleMode.fromString(
                                OceanBaseConnectionProvider.super.getCompatibleMode());
                OceanBaseDialect dialect =
                        compatibleMode.isMySqlMode()
                                ? new OceanBaseMySQLDialect()
                                : new OceanBaseOracleDialect();
                String version = null;
                try {
                    version = OceanBaseConnectionProvider.super.getVersion(dialect);
                } catch (SQLSyntaxErrorException e) {
                    if (!e.getMessage().contains("not exist")) {
                        throw e;
                    }
                }
                connectionInfo =
                        new OceanBaseConnectionInfo(options, compatibleMode, dialect, version);
            } catch (SQLException e) {
                throw new RuntimeException("Failed to get connection info", e);
            }
        }
        return connectionInfo;
    }

    @Override
    public OceanBaseTablePartInfo getTablePartInfo() {
        if (tablePartitionInfo == null) {
            TableEntry tableEntry;
            try (Connection connection = getConnection()) {
                if (getConnectionInfo().getVersion().isV4()) {
                    tableEntry =
                            new TableEntryExtractorV4()
                                    .queryTableEntry(
                                            connection, getConnectionInfo().getTableEntryKey());
                } else {
                    tableEntry =
                            new TableEntryExtractor()
                                    .queryTableEntry(
                                            connection, getConnectionInfo().getTableEntryKey());
                }
                if (tableEntry == null) {
                    throw new RuntimeException(
                            "Failed to get table entry with key: "
                                    + getConnectionInfo().getTableEntryKey());
                }
                tablePartitionInfo =
                        new OceanBaseTablePartInfo(
                                tableEntry, getConnectionInfo().getVersion().isV4());
            } catch (Exception e) {
                throw new RuntimeException("Failed to get table partition info", e);
            }
        }
        return tablePartitionInfo;
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
