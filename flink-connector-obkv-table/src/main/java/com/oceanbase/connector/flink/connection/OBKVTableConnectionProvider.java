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

import com.oceanbase.connector.flink.OBKVTableConnectorOptions;
import com.oceanbase.connector.flink.table.TableId;
import com.oceanbase.connector.flink.utils.TableCache;

import com.alipay.oceanbase.rpc.ObTableClient;

public class OBKVTableConnectionProvider implements ConnectionProvider {

    private static final long serialVersionUID = 1L;

    private final OBKVTableConnectorOptions options;

    private final TableCache<ObTableClient> tableCache;

    public OBKVTableConnectionProvider(OBKVTableConnectorOptions options) {
        this.options = options;
        this.tableCache = new TableCache<>();
    }

    public ObTableClient getTableClient(TableId tableId) {
        return tableCache.get(
                tableId.identifier(),
                () -> {
                    try {
                        ObTableClient tableClient = new ObTableClient();
                        tableClient.setParamURL(options.getUrl());
                        tableClient.setFullUserName(options.getUsername());
                        tableClient.setPassword(options.getPassword());
                        tableClient.setSysUserName(options.getSysUsername());
                        tableClient.setSysPassword(options.getSysPassword());
                        tableClient.init();
                        return tableClient;
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to initialize ObTableClient", e);
                    }
                });
    }

    @Override
    public void close() throws Exception {
        for (ObTableClient client : tableCache.getAll()) {
            client.close();
        }
        tableCache.clear();
    }
}
