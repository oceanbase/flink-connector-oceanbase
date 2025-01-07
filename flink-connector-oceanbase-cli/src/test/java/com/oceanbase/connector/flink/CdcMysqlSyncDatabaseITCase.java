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

import com.oceanbase.connector.flink.tools.cdc.DatabaseSync;
import com.oceanbase.connector.flink.tools.cdc.mysql.MysqlDatabaseSync;

import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class CdcMysqlSyncDatabaseITCase extends OceanBaseMySQLTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(CdcMysqlSyncDatabaseITCase.class);

    private static final String MYSQL_TABLE_NAME = "test_history_text";

    private static final MySQLContainer<?> MYSQL_CONTAINER =
            new MySQLContainer<>("mysql:8.0.20")
                    .withConfigurationOverride("docker/mysql")
                    .withInitScript("sql/mysql-cdc.sql")
                    .withNetwork(NETWORK)
                    .withExposedPorts(3306)
                    .withDatabaseName("test")
                    .withUsername("root")
                    .withPassword("mysqlpw")
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

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
        Stream.of(CONFIG_SERVER, CONTAINER, MYSQL_CONTAINER).forEach(GenericContainer::stop);
    }

    @Test
    public void testCdcMysqlSyncOceanBase() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Map<String, String> flinkMap = new HashMap<>();
        flinkMap.put("execution.checkpointing.interval", "10s");
        flinkMap.put("pipeline.operator-chaining", "false");
        flinkMap.put("parallelism.default", "1");
        Configuration configuration = Configuration.fromMap(flinkMap);
        env.configure(configuration);

        Map<String, String> mysqlConfig = new HashMap<>();
        mysqlConfig.put(MySqlSourceOptions.DATABASE_NAME.key(), MYSQL_CONTAINER.getDatabaseName());
        mysqlConfig.put(MySqlSourceOptions.HOSTNAME.key(), MYSQL_CONTAINER.getHost());
        mysqlConfig.put(
                MySqlSourceOptions.PORT.key(),
                String.valueOf(MYSQL_CONTAINER.getMappedPort(MySQLContainer.MYSQL_PORT)));
        mysqlConfig.put(MySqlSourceOptions.USERNAME.key(), MYSQL_CONTAINER.getUsername());
        mysqlConfig.put(MySqlSourceOptions.PASSWORD.key(), MYSQL_CONTAINER.getPassword());
        mysqlConfig.put("jdbc.properties.use_ssl", "false");
        Configuration config = Configuration.fromMap(mysqlConfig);

        Map<String, String> sinkConfig = new HashMap<>();
        sinkConfig.put(OceanBaseConnectorOptions.USERNAME.key(), CONTAINER.getUsername());
        sinkConfig.put(OceanBaseConnectorOptions.PASSWORD.key(), CONTAINER.getPassword());
        sinkConfig.put(OceanBaseConnectorOptions.URL.key(), CONTAINER.getJdbcUrl());
        sinkConfig.put("sink.enable-delete", "false");
        Configuration sinkConf = Configuration.fromMap(sinkConfig);

        DatabaseSync databaseSync = new MysqlDatabaseSync();
        databaseSync
                .setEnv(env)
                .setDatabase(MYSQL_CONTAINER.getDatabaseName())
                .setConfig(config)
                .setTablePrefix(null)
                .setTableSuffix(null)
                .setIncludingTables(MYSQL_CONTAINER.getDatabaseName() + ".*")
                .setExcludingTables(null)
                .setIgnoreDefaultValue(false)
                .setSinkConfig(sinkConf)
                .setCreateTableOnly(false)
                .create();
        databaseSync.build();

        env.executeAsync(
                String.format(
                        "MySQL-OceanBase Database Sync: %s", MYSQL_CONTAINER.getDatabaseName()));

        List<String> expected = Arrays.asList("1,21131,ces1,21321", "2,21321,ces2,12321");

        waitingAndAssertTableCount(MYSQL_TABLE_NAME, expected.size());

        List<String> actual =
                queryTable(MYSQL_TABLE_NAME, Arrays.asList("itemid", "clock", "value", "ns"));

        assertEqualsInAnyOrder(expected, actual);
    }
}
