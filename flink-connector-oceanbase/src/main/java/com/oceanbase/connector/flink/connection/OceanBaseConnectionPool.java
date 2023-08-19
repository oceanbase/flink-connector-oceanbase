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
import com.oceanbase.partition.calculator.enums.ObServerMode;
import com.oceanbase.partition.calculator.helper.TableEntryExtractor;
import com.oceanbase.partition.calculator.helper.TableEntryExtractorV4;
import com.oceanbase.partition.calculator.model.TableEntry;
import com.oceanbase.partition.calculator.model.TableEntryKey;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.SQLException;

public class OceanBaseConnectionPool implements OceanBaseConnectionProvider {

    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseConnectionPool.class);

    private static final long serialVersionUID = 1L;

    private final OceanBaseConnectionOptions options;
    private transient volatile boolean inited = false;
    private transient OceanBaseConnectionInfo connectionInfo;
    private transient DataSource dataSource;
    private transient OceanBaseTablePartInfo tablePartitionInfo;

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
            OceanBaseConnectionInfo.CompatibleMode compatibleMode =
                    OceanBaseConnectionInfo.CompatibleMode.fromString(options.getCompatibleMode());
            OceanBaseDialect dialect =
                    compatibleMode.isMySqlMode()
                            ? new OceanBaseMySQLDialect()
                            : new OceanBaseOracleDialect();
            String versionString;
            try {
                versionString = getVersion(dialect);
            } catch (SQLException e) {
                throw new RuntimeException("Failed to query version of OceanBase", e);
            }
            OceanBaseConnectionInfo.Version version =
                    OceanBaseConnectionInfo.Version.fromString(versionString);
            TableEntryKey tableEntryKey =
                    new TableEntryKey(
                            options.getClusterName(),
                            options.getTenantName(),
                            options.getSchemaName(),
                            options.getTableName(),
                            compatibleMode.isMySqlMode()
                                    ? ObServerMode.fromMySql(versionString)
                                    : ObServerMode.fromOracle(versionString));
            connectionInfo = new OceanBaseConnectionInfo(dialect, version, tableEntryKey);
        }
        return connectionInfo;
    }

    @Override
    public OceanBaseTablePartInfo getTablePartInfo() {
        if (tablePartitionInfo == null) {
            OceanBaseConnectionInfo.Version version = getConnectionInfo().getVersion();
            if ((version.isV4() && "sys".equalsIgnoreCase(options.getTenantName()))
                    || (!version.isV4() && !"sys".equalsIgnoreCase(options.getTenantName()))) {
                LOG.warn(
                        "Can't query table entry on tenant {} for current version of OceanBase",
                        options.getTenantName());
                return null;
            }
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
