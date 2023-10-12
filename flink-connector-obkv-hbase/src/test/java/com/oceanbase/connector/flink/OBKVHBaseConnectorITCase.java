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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

public class OBKVHBaseConnectorITCase extends OceanBaseTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(OBKVHBaseConnectorITCase.class);

    public static final String CONFIG_URL =
            "http://127.0.0.1:8080/services?Action=ObRootServiceInfo&ObCluster=obcluster";

    private GenericContainer<?> configServer;

    @Before
    @Override
    public void before() {
        super.before();

        configServer =
                new GenericContainer<>("whhe/obconfigserver")
                        .withNetworkMode("host")
                        .withLogConsumer(new Slf4jLogConsumer(LOG));
        Startables.deepStart(configServer).join();

        try (Connection connection =
                        DriverManager.getConnection(
                                obServer.getJdbcUrl(),
                                obServer.getSysUsername(),
                                obServer.getSysPassword());
                Statement statement = connection.createStatement()) {
            statement.execute(String.format("alter system set obconfig_url = '%s'", CONFIG_URL));
        } catch (SQLException e) {
            throw new RuntimeException("Set config url failed", e);
        }
    }

    @After
    @Override
    public void after() {
        super.after();

        if (configServer != null) {
            configServer.close();
        }
    }

    @Test
    public void testSink() throws Exception {
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        execEnv.setParallelism(1);
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(
                        execEnv, EnvironmentSettings.newInstance().inStreamingMode().build());

        String hTable = "htable";
        String familyA = "family1";
        String familyB = "family2";

        String url = String.format("%s&database=%s", CONFIG_URL, obServer.getDatabaseName());
        String tableA = String.format("`%s`.`%s$%s`", obServer.getDatabaseName(), hTable, familyA);
        String tableB = String.format("`%s`.`%s$%s`", obServer.getDatabaseName(), hTable, familyB);

        tEnv.executeSql(
                String.format(
                        "CREATE TEMPORARY TABLE target ("
                                + " rowkey STRING,"
                                + " %s ROW<q1 INT>,"
                                + " %s ROW<q2 STRING, q3 INT>,"
                                + " PRIMARY KEY (rowkey) NOT ENFORCED"
                                + ") with ("
                                + "  'connector'='obkv-hbase',"
                                + "  'url'='%s',"
                                + "  'table-name'='%s',"
                                + "  'username'='%s',"
                                + "  'password'='%s',"
                                + "  'sys.username'='%s',"
                                + "  'sys.password'='%s'"
                                + ");",
                        familyA,
                        familyB,
                        url,
                        hTable,
                        obServer.getUsername(),
                        obServer.getPassword(),
                        obServer.getSysUsername(),
                        obServer.getSysPassword()));

        tEnv.executeSql(
                        String.format(
                                "INSERT INTO target VALUES %s, %s, %s, %s",
                                row("1", 1, "1", 1),
                                row("2", null, "2", null),
                                row("3", 3, null, null),
                                row("4", 4, "4", null)))
                .await();

        List<String> expectedA = Arrays.asList("1,q1,1", "3,q1,3", "4,q1,4");
        List<String> expectedB = Arrays.asList("1,q2,1", "2,q2,2", "4,q2,4", "1,q3,1");

        List<String> resultA = queryTable(tableA, "K,Q,V");

        assertEqualsInAnyOrder(expectedA, resultA);

        List<String> resultB = queryTable(tableB, "K,Q,V");

        assertEqualsInAnyOrder(expectedB, resultB);
    }

    private String row(String key, Integer q1, String q2, Integer q3) {
        return String.format(
                "(%s, ROW(%s), ROW(%s, %s))", string(key), integer(q1), string(q2), integer(q3));
    }

    private String integer(Number n) {
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
