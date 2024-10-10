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

package com.oceanbase.connector.flink;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;

import com.alipay.oceanbase.rpc.protocol.payload.impl.ObLoadDupActionType;

import java.io.Serializable;
import java.time.Duration;
import java.util.Map;

/** The connector options of {@linkplain flink-connector-oceanbase-directload} module. */
public class OBDirectLoadConnectorOptions implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final ConfigOption<String> HOST =
            ConfigOptions.key("host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Hostname used in direct load.");

    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port")
                    .intType()
                    .defaultValue(2882)
                    .withDescription("Rpc port number used in direct load.");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The username.");

    public static final ConfigOption<String> TENANT_NAME =
            ConfigOptions.key("tenant-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The tenant name.");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The password.");

    public static final ConfigOption<String> SCHEMA_NAME =
            ConfigOptions.key("schema-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The schema name or database name.");

    public static final ConfigOption<String> TABLE_NAME =
            ConfigOptions.key("table-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The table name.");

    public static final ConfigOption<Integer> PARALLEL =
            ConfigOptions.key("parallel")
                    .intType()
                    .defaultValue(8)
                    .withDescription("Parallelism of direct load.");

    public static final ConfigOption<Integer> BUFFER_SIZE =
            ConfigOptions.key("buffer-size")
                    .intType()
                    .defaultValue(1024)
                    .withDescription(
                            "The size of the buffer that is written to the OceanBase at one time.");

    public static final ConfigOption<Long> MAX_ERROR_ROWS =
            ConfigOptions.key("max-error-rows")
                    .longType()
                    .defaultValue(0L)
                    .withDescription("Maximum tolerable number of error rows.");

    public static final ConfigOption<ObLoadDupActionType> DUP_ACTION =
            ConfigOptions.key("dup-action")
                    .enumType(ObLoadDupActionType.class)
                    .defaultValue(ObLoadDupActionType.REPLACE)
                    .withDescription("Action when there is duplicated record in direct load.");

    public static final ConfigOption<Duration> TIMEOUT =
            ConfigOptions.key("timeout")
                    .durationType()
                    .defaultValue(Duration.ofDays(7))
                    .withDescription("Timeout for direct load task.");

    public static final ConfigOption<Duration> HEARTBEAT_TIMEOUT =
            ConfigOptions.key("heartbeat-timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(60))
                    .withDescription("Client heartbeat timeout in direct load task.");

    public static final ConfigOption<Duration> HEARTBEAT_INTERVAL =
            ConfigOptions.key("heartbeat-interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(10))
                    .withDescription("Client heartbeat interval in direct load task.");

    public static final ConfigOption<String> LOAD_METHOD =
            ConfigOptions.key("load-method")
                    .stringType()
                    .defaultValue("full")
                    .withDescription(
                            "The direct-load load mode: full, inc, inc_replace.\n"
                                    + "full: full direct load, default value.\n"
                                    + "inc: normal incremental direct load, primary key conflict check will be performed, observer-4.3.2 and above support, dupAction REPLACE is not supported for the time being.\n"
                                    + "inc_replace: special replace mode incremental direct load, no primary key conflict check will be performed, directly overwrite the old data (equivalent to the effect of replace), dupAction parameter will be ignored, observer-4.3.2 and above support.");

    public static final ConfigOption<Boolean> ENABLE_MULTI_NODE_WRITE =
            ConfigOptions.key("enable-multi-node-write")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Enable multi node write.");

    public static final ConfigOption<String> EXECUTION_ID =
            ConfigOptions.key("execution-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The direct-load execution id. This parameter takes effect only when the enable-multi-node-write parameter is true.");

    protected final ReadableConfig allConfig;

    public OBDirectLoadConnectorOptions(Map<String, String> config) {
        this.allConfig = Configuration.fromMap(config);
    }

    public String getDirectLoadHost() {
        return allConfig.get(HOST);
    }

    public int getDirectLoadPort() {
        return allConfig.get(PORT);
    }

    public String getUsername() {
        return allConfig.get(USERNAME);
    }

    public String getTenantName() {
        return allConfig.get(TENANT_NAME);
    }

    public String getPassword() {
        return allConfig.get(PASSWORD);
    }

    public String getSchemaName() {
        return allConfig.get(SCHEMA_NAME);
    }

    public String getTableName() {
        return allConfig.get(TABLE_NAME);
    }

    public int getDirectLoadParallel() {
        return allConfig.get(PARALLEL);
    }

    public int getBufferSize() {
        return allConfig.get(BUFFER_SIZE);
    }

    public long getDirectLoadMaxErrorRows() {
        return allConfig.get(MAX_ERROR_ROWS);
    }

    public ObLoadDupActionType getDirectLoadDupAction() {
        return allConfig.get(DUP_ACTION);
    }

    public long getDirectLoadTimeout() {
        return allConfig.get(TIMEOUT).toMillis();
    }

    public long getDirectLoadHeartbeatTimeout() {
        return allConfig.get(HEARTBEAT_TIMEOUT).toMillis();
    }

    public long getDirectLoadHeartbeatInterval() {
        return allConfig.get(HEARTBEAT_INTERVAL).toMillis();
    }

    public String getDirectLoadLoadMethod() {
        return allConfig.get(LOAD_METHOD);
    }

    public boolean getEnableMultiNodeWrite() {
        return allConfig.get(ENABLE_MULTI_NODE_WRITE);
    }

    public String getExecutionId() {
        return allConfig.get(EXECUTION_ID);
    }
}
