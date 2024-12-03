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

package com.oceanbase.cdc.tools.tests;

import com.oceanbase.connector.flink.OceanBaseConnectorOptions;
import com.oceanbase.connector.flink.OceanBaseMySQLTestBase;
import com.oceanbase.connector.flink.tools.cdc.DatabaseSync;
import com.oceanbase.connector.flink.tools.cdc.mysql.MysqlDatabaseSync;

import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.shaded.org.apache.commons.io.IOUtils;
import org.testcontainers.utility.DockerLoggerFactory;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

public class CdcMysqlSyncDatabaseCase extends OceanBaseMySQLTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(CdcMysqlSyncDatabaseCase.class);

    private static final String MYSQL_HOST = "localhost";
    private static final Integer MYSQL_PORT = 3306;
    private static final String MYSQL_USER_NAME = "root";
    private static final String MYSQL_USER_PASSWORD = "mysqlpw";
    private static final String MYSQL_DATABASE = "mysql_cdc";
    private static final String MYSQL_TABLE_NAME = "test_history_text";
    static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    @BeforeClass
    public static void setup() {
        CONFIG_SERVER.withLogConsumer(new Slf4jLogConsumer(LOG)).start();
        CONTAINER
                .withEnv("OB_CONFIGSERVER_ADDRESS", getConfigServerAddress())
                .withLogConsumer(new Slf4jLogConsumer(LOG))
                .start();
        MYSQL_CONTAINER.start();
    }

    @AfterClass
    public static void tearDown() {
        Stream.of(MYSQL_CONTAINER).forEach(GenericContainer::stop);
    }

    private static final MySqlContainer MYSQL_CONTAINER =
            new MySqlContainer()
                    .withConfigurationOverride("docker/server-gtids/my.cnf")
                    .withSetupSQL("sql/cdc.sql")
                    // .withNetwork(NETWORK)
                    .withNetworkAliases(MYSQL_HOST)
                    .withExposedPorts(MYSQL_PORT)
                    .withDatabaseName(MYSQL_DATABASE)
                    .withPassword(MYSQL_USER_PASSWORD)
                    .withEnv("TZ", "Asia/Shanghai")
                    .withLogConsumer(
                            new Slf4jLogConsumer(
                                    DockerLoggerFactory.getLogger("mysql-docker-image")));

    @Test
    public void testCdcMysqlSyncOceanBase() throws Exception {
        extractedCdcSync();
        checkResult();
    }

    private static void extractedCdcSync() throws Exception {
        // env.setParallelism(1);
        Map<String, String> flinkMap = new HashMap<>();
        flinkMap.put("execution.checkpointing.interval", "10s");
        flinkMap.put("pipeline.operator-chaining", "false");
        flinkMap.put("parallelism.default", "1");

        Configuration configuration = Configuration.fromMap(flinkMap);
        env.configure(configuration);

        String tablePrefix = "";
        String tableSuffix = "";
        Map<String, String> mysqlConfig = new HashMap<>();
        mysqlConfig.put(MySqlSourceOptions.DATABASE_NAME.key(), MYSQL_DATABASE);
        mysqlConfig.put(MySqlSourceOptions.HOSTNAME.key(), MYSQL_HOST);
        mysqlConfig.put(
                MySqlSourceOptions.PORT.key(),
                String.valueOf(MYSQL_CONTAINER.getMappedPort(MYSQL_PORT)));
        mysqlConfig.put(MySqlSourceOptions.USERNAME.key(), MYSQL_USER_NAME);
        mysqlConfig.put(MySqlSourceOptions.PASSWORD.key(), MYSQL_USER_PASSWORD);
        // add jdbc properties for MySQL
        mysqlConfig.put("jdbc.properties.use_ssl", "false");
        Configuration config = Configuration.fromMap(mysqlConfig);

        Map<String, String> sinkConfig = new HashMap<>();
        sinkConfig.put(OceanBaseConnectorOptions.USERNAME.key(), CONTAINER.getUsername());
        sinkConfig.put(OceanBaseConnectorOptions.PASSWORD.key(), CONTAINER.getPassword());
        sinkConfig.put(OceanBaseConnectorOptions.URL.key(), CONTAINER.getJdbcUrl());
        sinkConfig.put("sink.enable-delete", "false");
        Configuration sinkConf = Configuration.fromMap(sinkConfig);

        String includingTables = "test.*";
        String excludingTables = "";
        boolean ignoreDefaultValue = false;
        boolean ignoreIncompatible = false;
        DatabaseSync databaseSync = new MysqlDatabaseSync();
        databaseSync
                .setEnv(env)
                .setDatabase(MYSQL_DATABASE)
                .setConfig(config)
                .setTablePrefix(tablePrefix)
                .setTableSuffix(tableSuffix)
                .setIncludingTables(includingTables)
                .setExcludingTables(excludingTables)
                .setIgnoreDefaultValue(ignoreDefaultValue)
                .setSinkConfig(sinkConf)
                .setCreateTableOnly(false)
                .create();
        databaseSync.build();
        env.executeAsync(String.format("MySQL-Doris Database Sync: %s", MYSQL_DATABASE));
        checkResult();
        env.close();
    }

    static void checkResult() {
        String sourceSql = String.format("select * from %s order by 1", MYSQL_TABLE_NAME);
        String sinkSql = String.format("select * from %s order by 1", MYSQL_TABLE_NAME);
        try (Statement sourceStatement =
                        getConnection(
                                        getJdbcUrl(
                                                MYSQL_HOST,
                                                MYSQL_CONTAINER.getMappedPort(MYSQL_PORT),
                                                MYSQL_DATABASE),
                                        MYSQL_USER_NAME,
                                        MYSQL_USER_PASSWORD)
                                .createStatement(
                                        ResultSet.TYPE_SCROLL_INSENSITIVE,
                                        ResultSet.CONCUR_READ_ONLY);
                Statement sinkStatement =
                        getConnection(
                                        CONTAINER.getJdbcUrl(),
                                        CONTAINER.getUsername(),
                                        CONTAINER.getPassword())
                                .createStatement(
                                        ResultSet.TYPE_SCROLL_INSENSITIVE,
                                        ResultSet.CONCUR_READ_ONLY);
                ResultSet sourceResultSet = sourceStatement.executeQuery(sourceSql);
                ResultSet sinkResultSet = sinkStatement.executeQuery(sinkSql)) {
            Assertions.assertEquals(
                    sourceResultSet.getMetaData().getColumnCount(),
                    sinkResultSet.getMetaData().getColumnCount());
            while (sourceResultSet.next()) {
                if (sinkResultSet.next()) {
                    for (String column : getFieldNames()) {
                        Object source = sourceResultSet.getObject(column);
                        Object sink = sinkResultSet.getObject(column);
                        if (!Objects.deepEquals(source, sink)) {
                            InputStream sourceAsciiStream = sourceResultSet.getBinaryStream(column);
                            InputStream sinkAsciiStream = sinkResultSet.getBinaryStream(column);
                            String sourceValue =
                                    IOUtils.toString(sourceAsciiStream, StandardCharsets.UTF_8);
                            String sinkValue =
                                    IOUtils.toString(sinkAsciiStream, StandardCharsets.UTF_8);
                            Assertions.assertEquals(sourceValue, sinkValue);
                        }
                    }
                }
            }
            sourceResultSet.last();
            sinkResultSet.last();
        } catch (Exception e) {
            throw new RuntimeException("Compare result error", e);
        }
    }

    static String[] getFieldNames() {
        return new String[] {
            "itemid", "clock", "value", "ns",
        };
    }

    public static Connection getConnection(String jdbcUrl, String userName, String password)
            throws SQLException {
        return DriverManager.getConnection(jdbcUrl, userName, password);
    }

    public static String getJdbcUrl(String host, Integer port, String schema) {
        return "jdbc:mysql://"
                + host
                + ":"
                + port
                + "/"
                + schema
                + "?useUnicode=true&characterEncoding=UTF-8&useSSL=false";
    }
}
