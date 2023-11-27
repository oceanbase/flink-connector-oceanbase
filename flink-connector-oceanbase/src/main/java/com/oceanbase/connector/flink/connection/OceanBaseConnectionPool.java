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
import com.oceanbase.connector.flink.dialect.OceanBaseMySQLDialect;
import com.oceanbase.connector.flink.dialect.OceanBaseOracleDialect;
import com.oceanbase.partition.calculator.enums.ObServerMode;
import com.oceanbase.partition.calculator.helper.TableEntryExtractor;
import com.oceanbase.partition.calculator.model.TableEntry;
import com.oceanbase.partition.calculator.model.TableEntryKey;

import com.alibaba.druid.pool.DruidDataSource;
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
                    DruidDataSource druidDataSource = new DruidDataSource();
                    druidDataSource.setUrl(options.getUrl());
                    druidDataSource.setUsername(options.getUsername());
                    druidDataSource.setPassword(options.getPassword());
                    druidDataSource.configFromPropety(options.getConnectionPoolProperties());
                    dataSource = druidDataSource;
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
                tableEntry =
                        new TableEntryExtractor()
                                .queryTableEntry(
                                        connection,
                                        getConnectionInfo().getTableEntryKey(),
                                        getConnectionInfo().getVersion().isV4());
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
