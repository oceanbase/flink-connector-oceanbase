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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.testcontainers.lifecycle.Startables;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class OBKVHBaseConnectorITCase extends OceanBaseMySQLTestBase {

    @Override
    public Map<String, String> getOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("username", getUsername() + "#" + getClusterName());
        options.put("password", getPassword());
        options.put("schema-name", getSchemaName());
        return options;
    }

    @Test
    public void testSink() throws Exception {
        Map<String, String> options = getOptions();
        options.put("url", getSysParameter("obconfig_url"));
        options.put("sys.username", getSysUsername());
        options.put("sys.password", getSysPassword());

        testSinkToHTable(options);
    }

    @Test
    public void testSinkWithODP() throws Exception {
        createSysUser("proxyro", getSysPassword());
        try (OceanBaseProxyContainer odpContainer = createOdpContainer(getSysPassword())) {
            Startables.deepStart(Stream.of(odpContainer)).join();

            Map<String, String> options = getOptions();
            options.put("odp-mode", "true");
            options.put("url", odpContainer.getHost() + ":" + odpContainer.getRpcPort());

            testSinkToHTable(options);
        }
    }

    private void testSinkToHTable(Map<String, String> options) throws Exception {
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        execEnv.setParallelism(1);
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(
                        execEnv, EnvironmentSettings.newInstance().inStreamingMode().build());

        initialize("sql/htable.sql");

        tEnv.executeSql(
                "CREATE TEMPORARY TABLE target ("
                        + " rowkey STRING,"
                        + " family1 ROW<q1 INT>,"
                        + " family2 ROW<q2 STRING, q3 INT>,"
                        + " PRIMARY KEY (rowkey) NOT ENFORCED"
                        + ") with ("
                        + "  'connector'='obkv-hbase',"
                        + "  'table-name'='htable',"
                        + getOptionsString(options)
                        + ");");

        String insertSql =
                String.format(
                        "INSERT INTO target VALUES "
                                + "(%s, ROW(%s), ROW(%s, %s)), "
                                + "(%s, ROW(%s), ROW(%s, %s)), "
                                + "(%s, ROW(%s), ROW(%s, %s)), "
                                + "(%s, ROW(%s), ROW(%s, %s))",
                        string("1"),
                        integer(1),
                        string("1"),
                        integer(1),
                        string("2"),
                        integer(null),
                        string("2"),
                        integer(null),
                        string("3"),
                        integer(3),
                        string(null),
                        integer(null),
                        string("4"),
                        integer(4),
                        string("4"),
                        integer(null));

        tEnv.executeSql(insertSql).await();

        List<String> expected1 = Arrays.asList("1,q1,1", "3,q1,3", "4,q1,4");
        List<String> expected2 = Arrays.asList("1,q2,1", "1,q3,1", "2,q2,2", "4,q2,4");

        RowConverter rowConverter =
                (rs, columnCount) -> {
                    String k = Bytes.toString(rs.getBytes("K"));
                    String q = Bytes.toString(rs.getBytes("Q"));
                    byte[] bytes = rs.getBytes("V");
                    String v;
                    switch (q) {
                        case "q1":
                        case "q3":
                            v = String.valueOf(Bytes.toInt(bytes));
                            break;
                        case "q2":
                            v = Bytes.toString(bytes);
                            break;
                        default:
                            throw new RuntimeException("Unknown qualifier: " + q);
                    }
                    return k + "," + q + "," + v;
                };

        waitingAndAssertTableCount("htable$family1", expected1.size());
        waitingAndAssertTableCount("htable$family2", expected2.size());

        List<String> actual1 = queryHTable("htable$family1", rowConverter);
        assertEqualsInAnyOrder(expected1, actual1);

        List<String> actual2 = queryHTable("htable$family2", rowConverter);
        assertEqualsInAnyOrder(expected2, actual2);

        dropTables("htable$family1", "htable$family2");
    }

    protected String integer(Integer n) {
        if (n == null) {
            return "CAST(NULL AS INT)";
        }
        return n.toString();
    }

    protected String string(String s) {
        if (s == null) {
            return "CAST(NULL AS STRING)";
        }
        return "'" + s + "'";
    }
}
