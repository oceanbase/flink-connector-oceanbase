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

package com.oceanbase.connector.flink;

import com.oceanbase.connector.flink.connection.OBKVHBaseConnectionOptions;
import com.oceanbase.connector.flink.sink.OBKVHBaseStatementOptions;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

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
