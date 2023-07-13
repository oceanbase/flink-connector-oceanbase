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
