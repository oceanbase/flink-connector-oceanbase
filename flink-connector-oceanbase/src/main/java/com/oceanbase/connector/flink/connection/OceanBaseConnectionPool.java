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

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;

public class OceanBaseConnectionPool implements OceanBaseConnectionProvider, Serializable {

    private static final long serialVersionUID = 1L;

    private final OceanBaseConnectionOptions options;
    private DruidDataSource dataSource;
    private volatile boolean inited = false;

    public OceanBaseConnectionPool(OceanBaseConnectionOptions options) {
        this.options = options;
    }

    public void init() throws SQLException {
        if (!inited) {
            synchronized (this) {
                if (!inited) {
                    dataSource = new DruidDataSource();
                    dataSource.setUrl(options.getUrl());
                    dataSource.setUsername(options.getUsername());
                    dataSource.setPassword(options.getPassword());
                    dataSource.setDriverClassName(options.getDriverClass());
                    dataSource.configFromPropety(options.getConnectionProperties());
                    dataSource.init();
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
    public void close() {
        if (dataSource != null) {
            dataSource.close();
            dataSource = null;
        }
        inited = false;
    }
}
