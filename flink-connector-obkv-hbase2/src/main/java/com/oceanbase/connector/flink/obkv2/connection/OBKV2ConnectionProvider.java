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

package com.oceanbase.connector.flink.obkv2.connection;

import com.oceanbase.connector.flink.obkv2.OBKVHBase2ConnectorOptions;

import com.alipay.oceanbase.hbase.OHTableClient;
import com.alipay.oceanbase.hbase.constants.OHConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/** Connection provider for OBKV HBase2 connector. */
public class OBKV2ConnectionProvider implements Serializable, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(OBKV2ConnectionProvider.class);
    private static final long serialVersionUID = 1L;

    private final OBKVHBase2ConnectorOptions connectorOptions;
    private final Map<String, Table> tableCache;

    public OBKV2ConnectionProvider(OBKVHBase2ConnectorOptions connectorOptions) {
        this.connectorOptions = connectorOptions;
        this.tableCache = new HashMap<>();
    }

    /**
     * Get or create HBase table client for the given table name.
     *
     * @return the HBase table client
     */
    public synchronized Table getHTableClient() {
        String tableName = connectorOptions.getTableName();
        return tableCache.computeIfAbsent(
                tableName,
                key -> {
                    try {
                        LOG.info("Creating OHTableClient for table: {}", tableName);
                        OHTableClient tableClient = new OHTableClient(tableName, getConfig());
                        tableClient.init();
                        return tableClient;
                    } catch (Exception e) {
                        throw new RuntimeException(
                                "Failed to initialize OHTableClient for " + tableName, e);
                    }
                });
    }

    private Configuration getConfig() {
        Configuration conf = new Configuration();

        if (connectorOptions.getOdpMode()) {
            // ODP mode configuration
            conf.setBoolean(OHConstants.HBASE_OCEANBASE_ODP_MODE, true);
            conf.set(OHConstants.HBASE_OCEANBASE_ODP_ADDR, connectorOptions.getOdpIp());
            conf.setInt(OHConstants.HBASE_OCEANBASE_ODP_PORT, connectorOptions.getOdpPort());
            conf.set(OHConstants.HBASE_OCEANBASE_DATABASE, connectorOptions.getSchemaName());
            LOG.info(
                    "Using ODP mode: {}:{}",
                    connectorOptions.getOdpIp(),
                    connectorOptions.getOdpPort());
        } else {
            // Direct mode configuration
            String paramUrl =
                    String.format(
                            "%s&database=%s",
                            connectorOptions.getUrl(), connectorOptions.getSchemaName());
            LOG.info("Using direct mode with paramURL: {}", paramUrl);
            conf.set(OHConstants.HBASE_OCEANBASE_PARAM_URL, paramUrl);
            conf.set(OHConstants.HBASE_OCEANBASE_SYS_USER_NAME, connectorOptions.getSysUsername());
            conf.set(OHConstants.HBASE_OCEANBASE_SYS_PASSWORD, connectorOptions.getSysPassword());
        }

        // Common configuration
        conf.set(OHConstants.HBASE_OCEANBASE_FULL_USER_NAME, connectorOptions.getUsername());
        conf.set(OHConstants.HBASE_OCEANBASE_PASSWORD, connectorOptions.getPassword());

        // Additional HBase properties
        Properties hbaseProperties = connectorOptions.getHbaseProperties();
        if (hbaseProperties != null) {
            for (String name : hbaseProperties.stringPropertyNames()) {
                conf.set(name, hbaseProperties.getProperty(name));
                LOG.debug("Set HBase property: {} = {}", name, hbaseProperties.getProperty(name));
            }
        }

        return conf;
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing OBKV2ConnectionProvider, {} table(s) in cache", tableCache.size());
        for (Map.Entry<String, Table> entry : tableCache.entrySet()) {
            try {
                LOG.debug("Closing table: {}", entry.getKey());
                entry.getValue().close();
            } catch (Exception e) {
                LOG.error("Failed to close table: {}", entry.getKey(), e);
            }
        }
        tableCache.clear();
    }
}
