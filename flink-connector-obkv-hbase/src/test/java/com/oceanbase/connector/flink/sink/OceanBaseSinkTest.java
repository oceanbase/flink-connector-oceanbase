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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

@Ignore
public class OceanBaseSinkTest {
    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseSinkTest.class);

    private static final String JDBC_URL = "";
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
        String family = "family1";

        String url = String.format("%s&database=%s", CONFIG_URL, schemaName);
        String fullTableName = tableName + "$" + family;

        String createTableSql =
                "CREATE TABLE %s ("
                        + "  K varbinary(1024),"
                        + "  Q varbinary(256),"
                        + "  T bigint,"
                        + "  V varbinary(1048576) NOT NULL,"
                        + "  PRIMARY KEY(K, Q, T))";

        try (Connection connection = DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD);
                Statement statement = connection.createStatement()) {
            statement.execute(String.format("DROP TABLE IF EXISTS %s", fullTableName));
            statement.execute(String.format(createTableSql, fullTableName));
        }

        tEnv.executeSql(
                String.format(
                        "CREATE TEMPORARY TABLE target ("
                                + " rowkey STRING,"
                                + " %s ROW<column1 STRING, column2 STRING>,"
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
                        family, url, tableName, USERNAME, PASSWORD, SYS_USERNAME, SYS_PASSWORD));

        StringBuilder sb = new StringBuilder("insert into target values ");
        for (int i = 0; i < 100; i++) {
            if (i != 0) {
                sb.append(",");
            }
            sb.append(
                    String.format(
                            "(%s, ROW(%s, %s))",
                            "row" + i, "row" + i + "_col1", "row" + i + "_col2"));
        }
        tEnv.executeSql(sb.toString()).await();

        try (Connection connection = DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD);
                Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery(String.format("SELECT * FROM %s", fullTableName));
            ResultSetMetaData metaData = rs.getMetaData();
            int count = 0;
            while (rs.next()) {
                sb = new StringBuilder("Row ").append(count++).append(": { ");
                for (int i = 0; i < metaData.getColumnCount(); i++) {
                    if (i != 0) {
                        sb.append(", ");
                    }
                    sb.append(metaData.getColumnName(i + 1))
                            .append(": ")
                            .append(rs.getObject(i + 1));
                }
                LOG.info(sb.append("}").toString());
            }
        }
    }
}
