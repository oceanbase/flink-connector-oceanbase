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

import com.oceanbase.connector.flink.source.cdc.CdcSync;
import com.oceanbase.connector.flink.source.cdc.CdcSyncConfig;
import com.oceanbase.connector.flink.source.cdc.mysql.MysqlCdcSync;

import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class CdcCli {
    private static final Logger LOG = LoggerFactory.getLogger(CdcCli.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Starting CdcCli with args: {}", Arrays.toString(args));

        String jobType = args[0];
        String[] opArgs = Arrays.copyOfRange(args, 1, args.length);
        MultipleParameterTool params = MultipleParameterTool.fromArgs(opArgs);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        CdcSync cdcSync;
        switch (jobType.trim().toLowerCase()) {
            case CdcSyncConfig.MYSQL_CDC:
                cdcSync = new MysqlCdcSync();
                break;
            default:
                throw new RuntimeException("Unsupported job type: " + jobType);
        }

        Map<String, String> sourceConfigMap = getConfigMap(params, CdcSyncConfig.SOURCE_CONF);
        Configuration sourceConfig = Configuration.fromMap(sourceConfigMap);

        Map<String, String> sinkConfigMap = getConfigMap(params, CdcSyncConfig.SINK_CONF);
        Configuration sinkConfig = Configuration.fromMap(sinkConfigMap);

        String jobName = params.get(CdcSyncConfig.JOB_NAME);
        String database = params.get(CdcSyncConfig.DATABASE);
        String tablePrefix = params.get(CdcSyncConfig.TABLE_PREFIX);
        String tableSuffix = params.get(CdcSyncConfig.TABLE_SUFFIX);
        String includingTables = params.get(CdcSyncConfig.INCLUDING_TABLES);
        String excludingTables = params.get(CdcSyncConfig.EXCLUDING_TABLES);
        String multiToOneOrigin = params.get(CdcSyncConfig.MULTI_TO_ONE_ORIGIN);
        String multiToOneTarget = params.get(CdcSyncConfig.MULTI_TO_ONE_TARGET);

        boolean createTableOnly = params.has(CdcSyncConfig.CREATE_TABLE_ONLY);
        boolean ignoreDefaultValue = params.has(CdcSyncConfig.IGNORE_DEFAULT_VALUE);
        boolean ignoreIncompatible = params.has(CdcSyncConfig.IGNORE_INCOMPATIBLE);

        cdcSync.setEnv(env)
                .setSourceConfig(sourceConfig)
                .setSinkConfig(sinkConfig)
                .setDatabase(database)
                .setTablePrefix(tablePrefix)
                .setTableSuffix(tableSuffix)
                .setIncludingTables(includingTables)
                .setExcludingTables(excludingTables)
                .setMultiToOneOrigin(multiToOneOrigin)
                .setMultiToOneTarget(multiToOneTarget)
                .setCreateTableOnly(createTableOnly)
                .setIgnoreDefaultValue(ignoreDefaultValue)
                .setIgnoreIncompatible(ignoreIncompatible)
                .build();

        if (StringUtils.isNullOrWhitespaceOnly(jobName)) {
            jobName = String.format("%s Sync", jobType);
        }
        env.execute(jobName);
    }

    public static Map<String, String> getConfigMap(MultipleParameterTool params, String key) {
        if (!params.has(key)) {
            throw new RuntimeException("Failed to find config by key: " + key);
        }

        Map<String, String> map = new HashMap<>();
        for (String param : params.getMultiParameter(key)) {
            String[] kv = param.split("=", 2);
            if (kv.length == 2) {
                map.put(kv[0].trim(), kv[1].trim());
                continue;
            }
            throw new RuntimeException("Invalid option: " + param);
        }
        if (map.isEmpty()) {
            throw new RuntimeException("Failed to get config by key: " + key);
        }
        return map;
    }
}
