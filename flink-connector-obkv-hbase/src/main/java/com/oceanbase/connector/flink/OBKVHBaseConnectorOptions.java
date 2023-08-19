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

package com.oceanbase.connector.flink;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import com.oceanbase.connector.flink.connection.OBKVHBaseConnectionOptions;
import com.oceanbase.connector.flink.sink.OBKVHBaseStatementOptions;

import java.util.Map;

public class OBKVHBaseConnectorOptions extends AbstractOceanBaseConnectorOptions {

    public static final ConfigOption<String> HBASE_PROPERTIES =
            ConfigOptions.key("hbase.properties")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Properties to configure 'obkv-hbase-client-java'.");

    private static final long serialVersionUID = 1L;

    public OBKVHBaseConnectorOptions(Map<String, String> config) {
        super(config);
    }

    public OBKVHBaseConnectionOptions getConnectionOptions() {
        return new OBKVHBaseConnectionOptions(
                allConfig.get(URL),
                allConfig.get(TABLE_NAME),
                allConfig.get(USERNAME),
                allConfig.get(PASSWORD),
                allConfig.get(SYS_USERNAME),
                allConfig.get(SYS_PASSWORD),
                parseProperties(allConfig.get(HBASE_PROPERTIES)));
    }

    public OBKVHBaseStatementOptions getStatementOptions() {
        return new OBKVHBaseStatementOptions(allConfig.get(BUFFER_BATCH_SIZE));
    }
}
