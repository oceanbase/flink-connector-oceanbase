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

import com.oceanbase.connector.flink.dialect.OceanBaseDialect;
import com.oceanbase.connector.flink.dialect.OceanBaseMySQLDialect;
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

    private static final OceanBaseDialect DIALECT = new OceanBaseMySQLDialect();

    private static final String JDBC_URL = "";
    private static final String CLUSTER_NAME = "";
    private static final String TENANT_NAME = "";
    private static final String USERNAME = "";
    private static final String PASSWORD = "";

    @Test
    public void testSink() throws Exception {
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        execEnv.setParallelism(1);
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(
                        execEnv, EnvironmentSettings.newInstance().inStreamingMode().build());

        String schemaName = "test";
        String tableName = "user";

        String createTableSql =
                "CREATE TABLE %s ("
                        + "id bigint(10) primary key,"
                        + "name varchar(20),"
                        + "age int(10),"
                        + "height double,"
                        + "birthday date)";

        String fullTableName = DIALECT.getFullTableName(schemaName, tableName);

        try (Connection connection = DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD);
                Statement statement = connection.createStatement()) {
            statement.execute(String.format("DROP TABLE IF EXISTS %s", fullTableName));
            statement.execute(String.format(createTableSql, fullTableName));
        }

        tEnv.executeSql(
                String.format(
                        "CREATE TEMPORARY TABLE target ("
                                + "    id  BIGINT,"
                                + "    name STRING,"
                                + "    age  INT,"
                                + "    height  DOUBLE,"
                                + "    birthday  STRING,"
                                + "    PRIMARY KEY (id) NOT ENFORCED"
                                + ") with ("
                                + "  'connector'='oceanbase',"
                                + "  'url'='%s',"
                                + "  'cluster-name'='%s',"
                                + "  'tenant-name'='%s',"
                                + "  'schema-name'='%s',"
                                + "  'table-name'='%s',"
                                + "  'username'='%s',"
                                + "  'password'='%s',"
                                + "  'compatible-mode'='mysql',"
                                + "  'driver-class'='com.mysql.jdbc.Driver',"
                                + "  'connection-pool'='druid',"
                                + "  'connection-pool-properties'='druid.initialSize=4;druid.maxActive=20;',"
                                + "  'partition.enabled'='true'"
                                + ");",
                        JDBC_URL,
                        CLUSTER_NAME,
                        TENANT_NAME,
                        schemaName,
                        tableName,
                        USERNAME,
                        PASSWORD));

        tEnv.executeSql(
                        "insert into target values "
                                + "(1, 'Tom', 30, 1.84, '1993-04-10'),"
                                + "(2, 'Jerry', 25, 1.65, '1997-10-29')")
                .await();

        try (Connection connection = DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD);
                Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery(String.format("SELECT * FROM %s", fullTableName));
            ResultSetMetaData metaData = rs.getMetaData();
            int count = 0;
            while (rs.next()) {
                StringBuilder sb = new StringBuilder("Row ").append(count++).append(": { ");
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
