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

import com.alipay.oceanbase.hbase.OHTableClient;
import org.apache.hadoop.hbase.client.HTableInterface;

import java.io.Serializable;

public class OBKVHBaseConnectionProvider implements Serializable, AutoCloseable {

    private static final long serialVersionUID = 1L;

    private final OBKVHBaseConnectionOptions options;

    private transient OHTableClient tableClient;

    public OBKVHBaseConnectionProvider(OBKVHBaseConnectionOptions options) {
        this.options = options;
    }

    public HTableInterface getTable() throws Exception {
        if (tableClient == null) {
            tableClient = new OHTableClient(options.getTableName(), options.getConfig());
            tableClient.init();
        }
        return tableClient;
    }

    @Override
    public void close() throws Exception {
        if (tableClient != null) {
            tableClient.close();
        }
    }
}
