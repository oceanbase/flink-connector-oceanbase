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
import com.oceanbase.connector.flink.table.TableId;
import com.oceanbase.connector.flink.utils.OceanBaseJdbcUtils;

import com.alibaba.druid.pool.DruidDataSource;
import com.alipay.oceanbase.rpc.table.ObDirectLoadParameter;
import com.alipay.oceanbase.rpc.table.ObTable;
import com.alipay.oceanbase.rpc.table.ObTableDirectLoad;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

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
                        : new OceanBaseOracleDialect();
    }

    private String getCompatibleMode() {
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

    public ObTableDirectLoad getDirectLoad(TableId tableId) {
        int count = OceanBaseJdbcUtils.getTableRowsCount(this::getConnection, tableId.identifier());
        if (count != 0) {
            throw new RuntimeException(
                    "Direct load can only work on empty table, while table "
                            + tableId.identifier()
                            + " has "
                            + count
                            + " rows");
        }
        ObTable table = getDirectLoadTable(tableId.getSchemaName());
        return new ObTableDirectLoad(table, tableId.getTableName(), getDirectLoadParameter(), true);
    }

    private ObTable getDirectLoadTable(String schemaName) {
        try {
            return new ObTable.Builder(options.getDirectLoadHost(), options.getDirectLoadPort())
                    .setLoginInfo(
                            getUserInfo().getTenant(),
                            getUserInfo().getUser(),
                            options.getPassword(),
                            schemaName)
                    .build();
        } catch (Exception e) {
            throw new RuntimeException("Failed to get ObTable", e);
        }
    }

    private ObDirectLoadParameter getDirectLoadParameter() {
        ObDirectLoadParameter parameter = new ObDirectLoadParameter();
        parameter.setParallel(options.getDirectLoadParallel());
        parameter.setMaxErrorRowCount(options.getDirectLoadMaxErrorRows());
        parameter.setDupAction(options.getDirectLoadDupAction());
        parameter.setTimeout(options.getDirectLoadTimeout());
        parameter.setHeartBeatTimeout(options.getDirectLoadHeartbeatTimeout());
        return parameter;
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
