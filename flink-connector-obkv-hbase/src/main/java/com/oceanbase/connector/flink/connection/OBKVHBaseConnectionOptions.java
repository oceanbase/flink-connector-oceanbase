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

import com.alipay.oceanbase.hbase.constants.OHConstants;
import org.apache.hadoop.conf.Configuration;

import java.io.Serializable;
import java.util.Properties;

public class OBKVHBaseConnectionOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String url;
    private final String tableName;
    private final String username;
    private final String password;
    private final String sysUsername;
    private final String sysPassword;
    private final Properties properties;

    public OBKVHBaseConnectionOptions(
            String url,
            String tableName,
            String username,
            String password,
            String sysUsername,
            String sysPassword,
            Properties properties) {
        this.url = url;
        this.tableName = tableName;
        this.username = username;
        this.password = password;
        this.sysUsername = sysUsername;
        this.sysPassword = sysPassword;
        this.properties = properties;
    }

    public String getTableName() {
        return tableName;
    }

    public Configuration getConfig() {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set(OHConstants.HBASE_OCEANBASE_PARAM_URL, url);
        conf.set(OHConstants.HBASE_OCEANBASE_FULL_USER_NAME, username);
        conf.set(OHConstants.HBASE_OCEANBASE_PASSWORD, password);
        conf.set(OHConstants.HBASE_OCEANBASE_SYS_USER_NAME, sysUsername);
        conf.set(OHConstants.HBASE_OCEANBASE_SYS_PASSWORD, sysPassword);
        for (String name : properties.stringPropertyNames()) {
            conf.set(name, properties.getProperty(name));
        }
        return conf;
    }
}
