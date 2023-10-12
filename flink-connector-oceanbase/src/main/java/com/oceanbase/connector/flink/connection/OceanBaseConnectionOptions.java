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
