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

package com.oceanbase.connector.flink.sink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class OceanBaseSinkTest {

    public static String CONFIG_URL = "";
    public static String USERNAME = "";
    public static String PASSWORD = "";
    public static String SYS_USERNAME = "";
    public static String SYS_PASSWORD = "";

    @Test
    public void testSink() throws Exception {
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        execEnv.setParallelism(1);
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(
                        execEnv, EnvironmentSettings.newInstance().inStreamingMode().build());
        String schemaName = "test";
        String tableName = "htable1";

        String url = String.format("%s&database=%s", CONFIG_URL, schemaName);

        // CREATE TABLE htable1$family1 (
        //  K varbinary(1024),
        //  Q varbinary(256),
        //  T bigint,
        //  V varbinary(1048576) NOT NULL,
        //  PRIMARY KEY(K, Q, T));

        tEnv.executeSql(
                String.format(
                        "CREATE TEMPORARY TABLE target ("
                                + " rowkey STRING,"
                                + " family1 ROW<column1 STRING, column2 STRING>,"
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
                        url, tableName, USERNAME, PASSWORD, SYS_USERNAME, SYS_PASSWORD));

        tEnv.executeSql(
                        "insert into target values "
                                + "('row1', ROW('r1c1', 'r1c2')),"
                                + "('row2', ROW('r2c1', 'r2c2'))")
                .await();
    }
}
