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

import java.io.Serializable;
import java.util.Properties;

public class OceanBaseConnectionOptions implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String url;
    private final String clusterName;
    private final String tenantName;
    private final String schemaName;
    private final String tableName;
    private final String username;
    private final String password;
    private final String compatibleMode;
    private final Properties connectionPoolProperties;

    public OceanBaseConnectionOptions(
            String url,
            String clusterName,
            String tenantName,
            String schemaName,
            String tableName,
            String username,
            String password,
            String compatibleMode,
            Properties connectionPoolProperties) {
        this.url = url;
        this.clusterName = clusterName;
        this.tenantName = tenantName;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.username = username;
        this.password = password;
        this.compatibleMode = compatibleMode;
        this.connectionPoolProperties = connectionPoolProperties;
    }

    public String getUrl() {
        return url;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getTenantName() {
        return tenantName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getCompatibleMode() {
        return compatibleMode;
    }

    public Properties getConnectionPoolProperties() {
        return connectionPoolProperties;
    }
}
