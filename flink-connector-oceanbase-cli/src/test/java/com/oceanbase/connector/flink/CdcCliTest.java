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

import com.oceanbase.connector.flink.cdc.DatabaseSyncConfig;

import org.apache.flink.api.java.utils.MultipleParameterTool;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class CdcCliTest {

    @Test
    public void getConfigMapTest() {
        MultipleParameterTool params =
                MultipleParameterTool.fromArgs(
                        new String[] {
                            "--sink-conf",
                            "password=password",
                            "--sink-conf",
                            "username=username",
                            "--sink-conf",
                            "url=jdbc:mysql://127.0.0.1:2881 "
                        });
        Map<String, String> sinkConf = CdcCli.getConfigMap(params, DatabaseSyncConfig.SINK_CONF);

        Map<String, String> excepted = new HashMap<>();
        excepted.put("password", "password");
        excepted.put("username", "username");
        excepted.put("url", "jdbc:mysql://127.0.0.1:2881");
        Assert.assertEquals(sinkConf, excepted);

        Map<String, String> mysqlConf = CdcCli.getConfigMap(params, DatabaseSyncConfig.MYSQL_CONF);
        Assert.assertNull(mysqlConf);
    }

    @Test
    public void testGetConfigMap() {
        Map<String, Collection<String>> config = new HashMap<>();
        config.put(
                DatabaseSyncConfig.MYSQL_CONF, Arrays.asList("  hostname=127.0.0.1", " port=3306"));
        config.put(
                DatabaseSyncConfig.POSTGRES_CONF,
                Arrays.asList("hostname=127.0.0.1 ", "port=5432 "));
        MultipleParameterTool parameter = MultipleParameterTool.fromMultiMap(config);
        Map<String, String> mysqlConfigMap =
                CdcCli.getConfigMap(parameter, DatabaseSyncConfig.MYSQL_CONF);
        Map<String, String> postGresConfigMap =
                CdcCli.getConfigMap(parameter, DatabaseSyncConfig.POSTGRES_CONF);

        Set<String> mysqlKeyConf = new HashSet<>(Arrays.asList("hostname", "port"));
        Set<String> mysqlValueConf = new HashSet<>(Arrays.asList("127.0.0.1", "3306"));
        assertEquals(mysqlConfigMap, mysqlKeyConf, mysqlValueConf);

        Set<String> postgresKeyConf = new HashSet<>(Arrays.asList("hostname", "port"));
        Set<String> postgresValueConf = new HashSet<>(Arrays.asList("127.0.0.1", "5432"));
        assertEquals(postGresConfigMap, postgresKeyConf, postgresValueConf);
    }

    private void assertEquals(
            Map<String, String> actualMap, Set<String> keyConf, Set<String> valueConf) {
        for (Entry<String, String> entry : actualMap.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            Assert.assertTrue(keyConf.contains(key));
            Assert.assertTrue(valueConf.contains(value));
        }
    }
}
