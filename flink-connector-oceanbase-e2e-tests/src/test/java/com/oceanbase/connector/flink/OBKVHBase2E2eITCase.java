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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

public class OBKVHBase2E2eITCase extends FlinkContainerTestEnvironment {

    private static final Logger LOG = LoggerFactory.getLogger(OBKVHBase2E2eITCase.class);

    private static final String SINK_CONNECTOR_NAME = "flink-sql-connector-obkv-hbase2.jar";

    @BeforeAll
    public static void setup() {
        CONFIG_SERVER.withLogConsumer(new Slf4jLogConsumer(LOG)).start();

        CONTAINER
                .withEnv("OB_CONFIGSERVER_ADDRESS", getConfigServerAddress())
                .withLogConsumer(new Slf4jLogConsumer(LOG))
                .start();
    }

    @AfterAll
    public static void tearDown() {
        Stream.of(CONFIG_SERVER, CONTAINER).forEach(GenericContainer::stop);
    }

    @BeforeEach
    public void before() throws Exception {
        super.before();

        initialize("sql/htable.sql");
    }

    @AfterEach
    public void after() throws Exception {
        super.after();

        dropTables("htable$family1");
    }

    @Test
    public void testInsertValues() throws Exception {
        List<String> sqlLines = new ArrayList<>();

        sqlLines.add("SET 'execution.checkpointing.interval' = '3s';");

        sqlLines.add(
                String.format(
                        "CREATE TEMPORARY TABLE target ("
                                + " rowkey STRING,"
                                + " q1 INT,"
                                + " PRIMARY KEY (rowkey) NOT ENFORCED"
                                + ") with ("
                                + "  'connector'='obkv-hbase2',"
                                + "  'url'='%s',"
                                + "  'sys.username'='%s',"
                                + "  'sys.password'='%s',"
                                + "  'username'='%s',"
                                + "  'password'='%s',"
                                + "  'schema-name'='%s',"
                                + "  'table-name'='htable',"
                                + "  'columnFamily'='family1'"
                                + ");",
                        getSysParameter("obconfig_url"),
                        getSysUsername(),
                        getSysPassword(),
                        getUsername() + "#" + getClusterName(),
                        getPassword(),
                        getSchemaName()));

        sqlLines.add(
                String.format(
                        "INSERT INTO target VALUES "
                                + "(%s, %s), "
                                + "(%s, %s), "
                                + "(%s, %s), "
                                + "(%s, %s);",
                        string("1"),
                        integer(1),
                        string("2"),
                        integer(2),
                        string("3"),
                        integer(3),
                        string("4"),
                        integer(4)));
        sqlLines.add("QUIT;");
        submitSQLJob(sqlLines, getResource(SINK_CONNECTOR_NAME));

        List<String> expected = Arrays.asList("1,q1,1", "2,q1,2", "3,q1,3", "4,q1,4");

        waitingAndAssertTableCount("htable$family1", expected.size());
    }
}
