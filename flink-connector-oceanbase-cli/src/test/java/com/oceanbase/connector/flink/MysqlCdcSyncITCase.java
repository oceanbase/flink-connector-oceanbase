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
import com.oceanbase.connector.flink.source.cdc.mysql.MysqlCdcSync;

import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class MysqlCdcSyncITCase extends OceanBaseMySQLTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(MysqlCdcSyncITCase.class);

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

    @BeforeAll
    public static void setup() {
        CONTAINER.withLogConsumer(new Slf4jLogConsumer(LOG)).start();
        MYSQL_CONTAINER.start();
    }

    @AfterAll
    public static void tearDown() {
        Stream.of(CONTAINER, MYSQL_CONTAINER).forEach(GenericContainer::stop);
    }

    @Test
    public void testMysqlCdcSync() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Map<String, String> flinkMap = new HashMap<>();
        flinkMap.put("execution.checkpointing.interval", "10s");
        flinkMap.put("pipeline.operator-chaining", "false");
        flinkMap.put("parallelism.default", "1");
        Configuration configuration = Configuration.fromMap(flinkMap);
        env.configure(configuration);

        // match all tables
        String tableName = ".*";

        Map<String, String> mysqlConfig = new HashMap<>();
        mysqlConfig.put(MySqlSourceOptions.HOSTNAME.key(), MYSQL_CONTAINER.getHost());
        mysqlConfig.put(
                MySqlSourceOptions.PORT.key(),
                String.valueOf(MYSQL_CONTAINER.getMappedPort(MySQLContainer.MYSQL_PORT)));
        mysqlConfig.put(MySqlSourceOptions.USERNAME.key(), MYSQL_CONTAINER.getUsername());
        mysqlConfig.put(MySqlSourceOptions.PASSWORD.key(), MYSQL_CONTAINER.getPassword());
        mysqlConfig.put(MySqlSourceOptions.DATABASE_NAME.key(), MYSQL_CONTAINER.getDatabaseName());
        mysqlConfig.put(MySqlSourceOptions.TABLE_NAME.key(), tableName);
        mysqlConfig.put("jdbc.properties.useSSL", "false");
        Configuration sourceConfig = Configuration.fromMap(mysqlConfig);

        Map<String, String> sinkConfig = new HashMap<>();
        sinkConfig.put(OceanBaseConnectorOptions.USERNAME.key(), CONTAINER.getUsername());
        sinkConfig.put(OceanBaseConnectorOptions.PASSWORD.key(), CONTAINER.getPassword());
        sinkConfig.put(OceanBaseConnectorOptions.URL.key(), CONTAINER.getJdbcUrl());
        sinkConfig.put("sink.enable-delete", "false");
        Configuration sinkConf = Configuration.fromMap(sinkConfig);

        CdcSync cdcSync = new MysqlCdcSync();
        cdcSync.setEnv(env)
                .setSourceConfig(sourceConfig)
                .setSinkConfig(sinkConf)
                .setDatabase(CONTAINER.getDatabaseName())
                .setIncludingTables(tableName)
                .build();

        env.executeAsync(
                String.format(
                        "MySQL-OceanBase Database Sync: %s -> %s",
                        MYSQL_CONTAINER.getDatabaseName(), CONTAINER.getDatabaseName()));

        waitingAndAssertTableCount("products", 9);
        waitingAndAssertTableCount("customers", 4);
    }
}
