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

import com.oceanbase.connector.flink.OBKVHBaseConnectorOptions;
import com.oceanbase.connector.flink.table.TableId;
import com.oceanbase.connector.flink.utils.OptionUtils;
import com.oceanbase.connector.flink.utils.TableCache;

import com.alipay.oceanbase.hbase.OHTableClient;
import com.alipay.oceanbase.hbase.constants.OHConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class OBKVHBaseConnectionProvider implements ConnectionProvider {

    private static final Logger LOG = LoggerFactory.getLogger(OBKVHBaseConnectionProvider.class);

    private static final long serialVersionUID = 1L;

    private final OBKVHBaseConnectorOptions options;

    private final TableCache<HTableInterface> tableCache;

    public OBKVHBaseConnectionProvider(OBKVHBaseConnectorOptions options) {
        this.options = options;
        this.tableCache = new TableCache<>();
    }

    public HTableInterface getHTableClient(TableId tableId) {
        return tableCache.get(
                tableId.identifier(),
                () -> {
                    try {
                        OHTableClient tableClient =
                                new OHTableClient(
                                        tableId.getTableName(), getConfig(tableId.getSchemaName()));
                        tableClient.init();
                        return tableClient;
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to initialize OHTableClient", e);
                    }
                });
    }

    private Configuration getConfig(String databaseName) {

        Configuration conf = new Configuration();
        if (options.getOdpMode()) {
            conf.setBoolean(OHConstants.HBASE_OCEANBASE_ODP_MODE, options.getOdpMode());
            conf.set(
                    OHConstants.HBASE_OCEANBASE_ODP_ADDR,
                    OptionUtils.getIpAndPort(options.getUrl())[0]);
            conf.setInt(
                    OHConstants.HBASE_OCEANBASE_ODP_PORT,
                    Integer.parseInt(OptionUtils.getIpAndPort(options.getUrl())[1]));
            conf.set(OHConstants.HBASE_OCEANBASE_DATABASE, databaseName);
        } else {
            String paramUrl = String.format("%s&database=%s", options.getUrl(), databaseName);
            LOG.debug("Set paramURL for database {} to {}", databaseName, paramUrl);
            conf.set(OHConstants.HBASE_OCEANBASE_PARAM_URL, paramUrl);
            conf.set(OHConstants.HBASE_OCEANBASE_SYS_USER_NAME, options.getSysUsername());
            conf.set(OHConstants.HBASE_OCEANBASE_SYS_PASSWORD, options.getSysPassword());
        }
        conf.set(OHConstants.HBASE_OCEANBASE_FULL_USER_NAME, options.getUsername());
        conf.set(OHConstants.HBASE_OCEANBASE_PASSWORD, options.getPassword());
        Properties hbaseProperties = options.getHBaseProperties();
        if (hbaseProperties != null) {
            for (String name : hbaseProperties.stringPropertyNames()) {
                conf.set(name, hbaseProperties.getProperty(name));
            }
        }
        return conf;
    }

    @Override
    public void close() throws Exception {
        for (HTableInterface table : tableCache.getAll()) {
            table.close();
        }
        tableCache.clear();
    }
}
