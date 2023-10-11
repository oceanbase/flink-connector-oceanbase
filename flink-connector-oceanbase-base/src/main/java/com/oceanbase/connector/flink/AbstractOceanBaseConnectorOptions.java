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

import com.oceanbase.connector.flink.sink.OceanBaseWriterOptions;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;

public abstract class AbstractOceanBaseConnectorOptions implements Serializable {

    public static final ConfigOption<String> URL =
            ConfigOptions.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The connection URL.");

    public static final ConfigOption<String> TABLE_NAME =
            ConfigOptions.key("table-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The table name.");

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

    public static final ConfigOption<String> SYS_USERNAME =
            ConfigOptions.key("sys.username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The username of system tenant.");

    public static final ConfigOption<String> SYS_PASSWORD =
            ConfigOptions.key("sys.password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The password of system tenant");

    public static final ConfigOption<Duration> BUFFER_FLUSH_INTERVAL =
            ConfigOptions.key("buffer-flush.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription(
                            "The flush interval mills, over this time, asynchronous threads will flush data.");

    public static final ConfigOption<Integer> BUFFER_SIZE =
            ConfigOptions.key("buffer-flush.buffer-size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Buffer size.");

    public static final ConfigOption<Integer> BUFFER_BATCH_SIZE =
            ConfigOptions.key("buffer-flush.batch-size")
                    .intType()
                    .defaultValue(100)
                    .withDescription("The flush batch size of records buffer.");

    public static final ConfigOption<Integer> MAX_RETRIES =
            ConfigOptions.key("max-retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription("The max retry times if writing records to database failed.");

    protected final ReadableConfig allConfig;

    protected void validateConfig() {}

    public AbstractOceanBaseConnectorOptions(Map<String, String> config) {
        this.allConfig = Configuration.fromMap(config);
        validateConfig();
    }

    public OceanBaseWriterOptions getWriterOptions() {
        return new OceanBaseWriterOptions(
                allConfig.get(BUFFER_FLUSH_INTERVAL).toMillis(),
                allConfig.get(BUFFER_SIZE),
                allConfig.get(MAX_RETRIES));
    }

    protected Properties parseProperties(String propsStr) {
        Properties props = new Properties();
        if (StringUtils.isBlank(propsStr)) {
            return props;
        }
        for (String propStr : propsStr.split(";")) {
            if (StringUtils.isBlank(propStr)) {
                continue;
            }
            String[] pair = propStr.trim().split("=");
            if (pair.length != 2) {
                throw new IllegalArgumentException("properties must have one key value pair");
            }
            props.put(pair[0].trim(), pair[1].trim());
        }
        return props;
    }
}
