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

import com.oceanbase.connector.flink.utils.FlinkContainerTestEnvironment;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.util.Collections;
import java.util.stream.Stream;

@DisabledIfSystemProperty(
        named = "flink_version",
        matches = "1.15.4",
        disabledReason = "Flink 1.15.4 does not contain 'SideOutputDataStream'")
public class MysqlCdcSyncE2eITCase extends FlinkContainerTestEnvironment {

    private static final Logger LOG = LoggerFactory.getLogger(MysqlCdcSyncE2eITCase.class);

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
        submitJob(
                Collections.singletonList(getResource("flink-sql-connector-mysql-cdc.jar")),
                getResource("flink-connector-oceanbase-cli.jar"),
                new String[] {
                    multipleParameterArg("source-type", "mysql-cdc"),
                    multipleParameterArg(
                            "source-conf", "hostname=" + getContainerIP(MYSQL_CONTAINER)),
                    multipleParameterArg("source-conf", "port=" + MySQLContainer.MYSQL_PORT),
                    multipleParameterArg(
                            "source-conf", "username=" + MYSQL_CONTAINER.getUsername()),
                    multipleParameterArg(
                            "source-conf", "password=" + MYSQL_CONTAINER.getPassword()),
                    multipleParameterArg(
                            "source-conf", "database-name=" + MYSQL_CONTAINER.getDatabaseName()),
                    multipleParameterArg("source-conf", "table-name=.*"),
                    multipleParameterArg("sink-conf", "url=" + getJdbcUrl()),
                    multipleParameterArg("sink-conf", "username=" + CONTAINER.getUsername()),
                    multipleParameterArg("sink-conf", "password=" + CONTAINER.getPassword()),
                    multipleParameterArg("job-name", "test-mysql-cdc-sync"),
                    multipleParameterArg("database", CONTAINER.getDatabaseName()),
                    multipleParameterArg("including-tables", ".*")
                });

        waitingAndAssertTableCount("products", 9);
        waitingAndAssertTableCount("customers", 4);
    }
}
