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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import com.alipay.oceanbase.hbase.OHTableClient;
import com.alipay.oceanbase.hbase.constants.OHConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.junit.Assert.assertTrue;

public class OBKVHBaseConnectorITCase extends OceanBaseTestBase {

    public static final String CLUSTER_NAME = "obcluster";
    public static final String CONFIG_URL =
            "http://127.0.0.1:8080/services?Action=ObRootServiceInfo&ObCluster=" + CLUSTER_NAME;

    @Override
    protected String getUrl() {
        return String.format("%s&database=%s", CONFIG_URL, OB_SERVER.getDatabaseName());
    }

    @Override
    protected String getUsername() {
        return OB_SERVER.getUsername() + "#" + CLUSTER_NAME;
    }

    @Override
    protected String getTestTable() {
        return "htable";
    }

    @Test
    public void testSink() throws Exception {
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        execEnv.setParallelism(1);
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(
                        execEnv, EnvironmentSettings.newInstance().inStreamingMode().build());

        String family1 = "family1";
        String family2 = "family2";

        tEnv.executeSql(
                String.format(
                        "CREATE TEMPORARY TABLE target ("
                                + " rowkey STRING,"
                                + " %s ROW<q1 INT>,"
                                + " %s ROW<q2 STRING, q3 INT>,"
                                + " PRIMARY KEY (rowkey) NOT ENFORCED"
                                + ") with ("
                                + "  'connector'='obkv-hbase',"
                                + "  'sys.username'='%s',"
                                + "  'sys.password'='%s',"
                                + getCommonOptionsString()
                                + ");",
                        family1,
                        family2,
                        OB_SERVER.getSysUsername(),
                        OB_SERVER.getSysPassword()));

        tEnv.executeSql(
                        String.format(
                                "INSERT INTO target VALUES %s, %s, %s, %s",
                                row("1", 1, "1", 1),
                                row("2", null, "2", null),
                                row("3", 3, null, null),
                                row("4", 4, "4", null)))
                .await();

        Configuration conf = new Configuration();
        conf.set(OHConstants.HBASE_OCEANBASE_PARAM_URL, getUrl());
        conf.set(OHConstants.HBASE_OCEANBASE_FULL_USER_NAME, getUsername());
        conf.set(OHConstants.HBASE_OCEANBASE_PASSWORD, getPassword());
        conf.set(OHConstants.HBASE_OCEANBASE_SYS_USER_NAME, OB_SERVER.getSysUsername());
        conf.set(OHConstants.HBASE_OCEANBASE_SYS_PASSWORD, OB_SERVER.getSysPassword());

        OHTableClient client = new OHTableClient(getTestTable(), conf);
        client.init();

        Function<KeyValue, String> valueFunc =
                kv -> {
                    String column = Bytes.toString(kv.getQualifier());
                    if ("q2".equals(column)) {
                        return Bytes.toString(kv.getValue());
                    } else {
                        return String.valueOf(Bytes.toInt(kv.getValue()));
                    }
                };

        assertEqualsInAnyOrder(
                Collections.singletonList("1,q1,1"), queryHTable(client, family1, "1", valueFunc));
        assertTrue(queryHTable(client, family1, "2", valueFunc).isEmpty());
        assertEqualsInAnyOrder(
                Collections.singletonList("3,q1,3"), queryHTable(client, family1, "3", valueFunc));
        assertEqualsInAnyOrder(
                Collections.singletonList("4,q1,4"), queryHTable(client, family1, "4", valueFunc));

        assertEqualsInAnyOrder(
                Arrays.asList("1,q2,1", "1,q3,1"), queryHTable(client, family2, "1", valueFunc));
        assertEqualsInAnyOrder(
                Collections.singletonList("2,q2,2"), queryHTable(client, family2, "2", valueFunc));
        assertTrue(queryHTable(client, family2, "3", valueFunc).isEmpty());
        assertEqualsInAnyOrder(
                Collections.singletonList("4,q2,4"), queryHTable(client, family2, "4", valueFunc));

        client.close();
    }

    private List<String> queryHTable(
            OHTableClient client,
            String family,
            String rowKey,
            Function<KeyValue, String> valueStringFunction)
            throws IOException {
        List<String> result = new ArrayList<>();
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addFamily(Bytes.toBytes(family));
        Result r = client.get(get);
        if (r == null || r.isEmpty()) {
            return result;
        }
        for (KeyValue kv : r.list()) {
            result.add(
                    String.format(
                            "%s,%s,%s",
                            rowKey,
                            Bytes.toString(kv.getQualifier()),
                            valueStringFunction.apply(kv)));
        }
        return result;
    }

    private String row(String key, Integer q1, String q2, Integer q3) {
        return String.format(
                "(%s, ROW(%s), ROW(%s, %s))", string(key), integer(q1), string(q2), integer(q3));
    }

    private String integer(Integer n) {
        if (n == null) {
            return "CAST(NULL AS INT)";
        }
        return n.toString();
    }

    private String string(String s) {
        if (s == null) {
            return "CAST(NULL AS STRING)";
        }
        return "'" + s + "'";
    }
}
