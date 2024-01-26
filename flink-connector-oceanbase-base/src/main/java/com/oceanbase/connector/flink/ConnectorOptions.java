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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;

import java.io.Serializable;
import java.time.Duration;
import java.util.Map;

public abstract class ConnectorOptions implements Serializable {

    public static final ConfigOption<String> URL =
            ConfigOptions.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The connection URL.");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The username.");

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

    public static final ConfigOption<Boolean> SYNC_WRITE =
            ConfigOptions.key("sync-write")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to write synchronously.");

    public static final ConfigOption<Duration> BUFFER_FLUSH_INTERVAL =
            ConfigOptions.key("buffer-flush.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription(
                            "The flush interval, over this time, asynchronous threads will flush data. Default value is '1s'. "
                                    + "If it's set to zero value like '0', scheduled flushing will be disabled.");

    public static final ConfigOption<Integer> BUFFER_SIZE =
            ConfigOptions.key("buffer-flush.buffer-size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Buffer size. Default value is '1000'.");

    public static final ConfigOption<Integer> MAX_RETRIES =
            ConfigOptions.key("max-retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            "The max retry times if writing records to database failed. Default value is '3'.");

    protected final ReadableConfig allConfig;

    public ConnectorOptions(Map<String, String> config) {
        this.allConfig = Configuration.fromMap(config);
    }

    public String getUrl() {
        return allConfig.get(URL);
    }

    public String getUsername() {
        return allConfig.get(USERNAME);
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

    public boolean getSyncWrite() {
        return allConfig.get(SYNC_WRITE);
    }

    public long getBufferFlushInterval() {
        return allConfig.get(BUFFER_FLUSH_INTERVAL).toMillis();
    }

    public int getBufferSize() {
        return allConfig.get(BUFFER_SIZE);
    }

    public int getMaxRetries() {
        return allConfig.get(MAX_RETRIES);
    }
}
