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
    private final String username;
    private final String password;
    private final Properties connectionProperties;

    public OceanBaseConnectionOptions(
            String url, String username, String password, Properties connectionProperties) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.connectionProperties = connectionProperties;
    }

    public String getUrl() {
        return url;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public Properties getConnectionProperties() {
        return connectionProperties;
    }
}
