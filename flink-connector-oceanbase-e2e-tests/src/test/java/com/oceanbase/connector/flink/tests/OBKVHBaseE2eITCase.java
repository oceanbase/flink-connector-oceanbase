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

package com.oceanbase.connector.flink.tests;

import com.oceanbase.connector.flink.tests.utils.FlinkContainerTestEnvironment;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

public class OBKVHBaseE2eITCase extends FlinkContainerTestEnvironment {

    private static final Logger LOG = LoggerFactory.getLogger(OBKVHBaseE2eITCase.class);

    private static final String SINK_CONNECTOR_NAME = "flink-sql-connector-obkv-hbase.jar";

    @BeforeClass
    public static void setup() {
        CONFIG_SERVER.withLogConsumer(new Slf4jLogConsumer(LOG)).start();

        CONTAINER
                .withEnv("OB_CONFIGSERVER_ADDRESS", getConfigServerAddress())
                .withLogConsumer(new Slf4jLogConsumer(LOG))
                .start();
    }

    @AfterClass
    public static void tearDown() {
        Stream.of(CONFIG_SERVER, CONTAINER).forEach(GenericContainer::stop);
    }

    @Override
    protected String getFlinkDockerImageTag() {
        // the hbase packages are not compatible with jdk 11
        return String.format("flink:%s-scala_2.12-java8", flinkVersion);
    }

    @Before
    public void before() throws Exception {
        super.before();

        initialize("sql/htable.sql");
    }

    @After
    public void after() throws Exception {
        super.after();

        dropTables("htable$family1", "htable$family2");
    }

    @Test
    public void testInsertValues() throws Exception {
        List<String> sqlLines = new ArrayList<>();

        sqlLines.add("SET 'execution.checkpointing.interval' = '3s';");

        sqlLines.add(
                String.format(
                        "CREATE TEMPORARY TABLE target ("
                                + " rowkey STRING,"
                                + " family1 ROW<q1 INT>,"
                                + " family2 ROW<q2 STRING, q3 INT>,"
                                + " PRIMARY KEY (rowkey) NOT ENFORCED"
                                + ") with ("
                                + "  'connector'='obkv-hbase',"
                                + "  'url'='%s',"
                                + "  'sys.username'='%s',"
                                + "  'sys.password'='%s',"
                                + "  'username'='%s',"
                                + "  'password'='%s',"
                                + "  'schema-name'='%s',"
                                + "  'table-name'='htable'"
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
                                + "(%s, ROW(%s), ROW(%s, %s)), "
                                + "(%s, ROW(%s), ROW(%s, %s)), "
                                + "(%s, ROW(%s), ROW(%s, %s)), "
                                + "(%s, ROW(%s), ROW(%s, %s));",
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
                        integer(null)));

        submitSQLJob(sqlLines, getResource(SINK_CONNECTOR_NAME));
        waitUntilJobRunning(Duration.ofSeconds(30));

        List<String> expected1 = Arrays.asList("1,q1,1", "3,q1,3", "4,q1,4");
        List<String> expected2 = Arrays.asList("1,q2,1", "1,q3,1", "2,q2,2", "4,q2,4");

        waitingAndAssertTableCount("htable$family1", expected1.size());
        waitingAndAssertTableCount("htable$family2", expected2.size());
    }
}
